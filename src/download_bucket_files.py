import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import List

import aioboto3
import obstore as obs
from httpx import AsyncClient
from aioaws.s3 import S3Client as AioAwsS3Client, S3Config as AioAwsS3Config
from obstore.store import S3Store

# --------- CONFIG FROM ENV ---------
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

# Bucket where you have at least List/Get permission.
S3_TEST_BUCKET = os.getenv("S3_TEST_BUCKET", "auto-product-build-downstream").strip()

# Default maximum number of objects to list per interface
DEFAULT_MAX_ITEMS = int(os.getenv("S3_LIST_LIMIT", "100"))


def _require_creds() -> None:
    missing = [
        name
        for name, value in [
            ("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID),
            ("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY),
        ]
        if not value
    ]
    if missing:
        print("Missing required env vars:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)


def _is_downloadable_entry(entry) -> bool:
    """
    Decide whether this entry represents a real S3 object (not a prefix).
    Handles:
      - aioboto3 dicts: {"Key": "...", "Size": 0/whatever}
      - aioaws ObjectMeta: obj.key, obj.size
      - obstore dicts: {"path": "...", "size": ...}
      - obstore ObjectMeta: entry.path, entry.size
    """
    # 1. aioboto3 paginator dict
    if isinstance(entry, dict) and "Key" in entry:
        key = entry["Key"]
        size = entry.get("Size", None)
        return not key.endswith("/") and (size is None or size > 0)

    # 2. aioaws ObjectMeta
    if hasattr(entry, "key") and hasattr(entry, "size"):
        return not entry.key.endswith("/") and entry.size > 0

    # 3. obstore dict format
    if isinstance(entry, dict) and "path" in entry:
        key = entry.get("path")
        size = entry.get("size", None)
        return (
            key is not None
            and not key.endswith("/")
            and (size is None or size > 0)
        )

    # 4. obstore ObjectMeta
    if hasattr(entry, "path") and hasattr(entry, "size"):
        return not entry.path.endswith("/") and entry.size > 0

    return False


# --------------------------------------------------------------------
# aioboto3: check *authentication only* via STS.GetCallerIdentity
# --------------------------------------------------------------------
async def check_aioboto3_sts() -> None:
    """
    Cleanest way to check that credentials are valid:
    STS.GetCallerIdentity doesn't require any S3 permissions.
    """
    print("\n=== aioboto3: STS GetCallerIdentity ===")
    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    async with session.client("sts") as sts:
        resp = await sts.get_caller_identity()
        print("  Account :", resp["Account"])
        print("  ARN     :", resp["Arn"])
        print("  UserId  :", resp["UserId"])
        print("  -> aioboto3 credentials are valid")


# --------------------------------------------------------------------
# aioaws: minimal S3 call on a specific bucket
# --------------------------------------------------------------------
async def check_aioaws_s3() -> None:
    print(f"\n=== aioaws: S3 list sanity check on bucket {S3_TEST_BUCKET!r} ===")

    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping aioaws S3 check.")
        return

    async with AsyncClient(timeout=10) as http_client:
        s3 = AioAwsS3Client(
            http_client,
            AioAwsS3Config(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_REGION,
                S3_TEST_BUCKET,
            ),
        )

        try:
            count = 0
            async for obj in s3.list():
                # obj has `.key` for the S3 object key
                print("  First key from list():", obj.key)
                count += 1
                if count >= 1:
                    break

            if count == 0:
                print("  list() returned no objects, but request was accepted.")
            print("  -> aioaws S3 request signed & accepted")
        except Exception as e:
            print("  aioaws S3 call failed:", repr(e))


# --------------------------------------------------------------------
# obstore: S3Store and list_async() on S3_TEST_BUCKET
# --------------------------------------------------------------------
async def check_obstore_s3() -> None:
    print(f"\n=== obstore: S3Store list_async sanity check on bucket {S3_TEST_BUCKET!r} ===")

    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping obstore S3 check.")
        return

    # S3Store will read creds from env; we pass region explicitly.
    store = S3Store(
        bucket=S3_TEST_BUCKET,
        prefix="",  # list from bucket root
        region=AWS_REGION,
    )

    try:
        # list_async() yields *batches* (Sequence[ObjectMeta] or Sequence[dict])
        batch_count = 0
        async for batch in store.list_async(prefix=""):
            batch_count += 1
            if not batch:
                continue

            first = batch[0]

            # Handle both older dict-based and newer ObjectMeta-based APIs
            if isinstance(first, dict):
                # Older API: dict with "path" / "size" etc.
                name = first.get("path") or first
            else:
                # Newer API: ObjectMeta with `.path`
                name = getattr(first, "path", str(first))

            print("  First path from list_async():", name)
            print("  -> obstore S3 request signed & accepted")
            break

        if batch_count == 0:
            print("  list_async() returned no batches, but request was accepted.")
    except Exception as e:
        msg = str(e)
        if "NoSuchBucket" in msg:
            print(
                "  -> credentials are OK, but bucket does not exist or is wrong:",
                S3_TEST_BUCKET,
            )
        else:
            print("  obstore S3 call failed:", msg)


# --------------------------------------------------------------------
# Full listings with a configurable max_items
# --------------------------------------------------------------------
async def list_aioboto3_contents(max_items: int = 100) -> List[str]:
    print(
        f"\n=== aioboto3: listing up to {max_items} objects (Downloading only files) "
        f"from bucket {S3_TEST_BUCKET!r} ==="
    )
    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping.")
        return []

    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    keys: List[str] = []

    async with session.client("s3") as s3:
        paginator = s3.get_paginator("list_objects_v2")
        count = 0

        async for page in paginator.paginate(Bucket=S3_TEST_BUCKET):
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj["Key"]
                print("  -", key)
                keys.append(key)
                count += 1
                if count >= max_items:
                    break
            if count >= max_items:
                break

        print(f"  -> aioboto3 listed {count} objects (limit {max_items}).")

    return keys


async def list_aioaws_contents(max_items: int = 100) -> List[str]:
    print(
        f"\n=== aioaws: listing up to {max_items} objects (Downloading only files) "
        f"from bucket {S3_TEST_BUCKET!r} ==="
    )
    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping.")
        return []

    keys: List[str] = []

    async with AsyncClient(timeout=30) as http_client:
        s3 = AioAwsS3Client(
            http_client,
            AioAwsS3Config(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_REGION,
                S3_TEST_BUCKET,
            ),
        )

        count = 0
        try:
            async for obj in s3.list():
                print("  -", obj.key)
                keys.append(obj.key)
                count += 1
                if count >= max_items:
                    break

            print(f"  -> aioaws listed {count} objects (limit {max_items}).")
        except Exception as e:
            print("  aioaws bucket listing failed:", e)

    return keys


async def list_obstore_contents(max_items: int = 100) -> List[str]:
    print(
        f"\n=== obstore: listing up to {max_items} objects (Downloading only files) "
        f"from bucket {S3_TEST_BUCKET!r} ==="
    )
    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping.")
        return []

    store = S3Store(
        bucket=S3_TEST_BUCKET,
        prefix="",
        region=AWS_REGION,
    )

    keys: List[str] = []
    count = 0
    try:
        async for batch in store.list_async(prefix=""):
            for entry in batch:
                if isinstance(entry, dict):
                    name = entry.get("path") or entry
                else:
                    name = getattr(entry, "path", str(entry))

                key = str(name)
                print("  -", key)
                keys.append(key)
                count += 1
                if count >= max_items:
                    break
            if count >= max_items:
                break

        print(f"  -> obstore listed {count} objects (limit {max_items}).")
    except Exception as e:
        print("  obstore bucket listing failed:", e)

    return keys


# --------------------------------------------------------------------
# Downloads for each interface
# --------------------------------------------------------------------
async def download_aioboto3_files(keys: List[str], outdir: Path) -> None:
    base = outdir / "aioboto3"
    base.mkdir(parents=True, exist_ok=True)
    print(f"\n=== aioboto3: downloading {len(keys)} objects into {base} ===")

    if not keys:
        print("  (no keys to download)")
        return

    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    async with session.client("s3") as s3:
        for key in keys:
            if not _is_downloadable_entry(key):
                continue

            local_path = base / key
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                resp = await s3.get_object(Bucket=S3_TEST_BUCKET, Key=key)
                body = resp["Body"]
                # aiobotocore-style streaming body
                async with body:
                    with open(local_path, "wb") as f:
                        while True:
                            chunk = await body.read(1 * 1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                print(f"  downloaded: {key} -> {local_path}")
            except Exception as e:
                print(f"  FAILED to download {key!r} via aioboto3:", e)


async def download_aioaws_files(keys: List[str], outdir: Path) -> None:
    base = outdir / "aioaws"
    base.mkdir(parents=True, exist_ok=True)
    print(f"\n=== aioaws: downloading {len(keys)} objects into {base} ===")

    if not keys:
        print("  (no keys to download)")
        return

    async with AsyncClient(timeout=60) as http_client:
        s3 = AioAwsS3Client(
            http_client,
            AioAwsS3Config(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_REGION,
                S3_TEST_BUCKET,
            ),
        )

        for key in keys:
            if not _is_downloadable_entry(key):
                continue

            local_path = base / key
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                # aioaws generates a signed download URL; we fetch via httpx
                url = s3.signed_download_url(key, max_age=3600)
                async with http_client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    with open(local_path, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            f.write(chunk)
                print(f"  downloaded: {key} -> {local_path}")
            except Exception as e:
                print(f"  FAILED to download {key!r} via aioaws:", e)


async def download_obstore_files(keys: List[str], outdir: Path) -> None:
    base = outdir / "obstore"
    base.mkdir(parents=True, exist_ok=True)
    print(f"\n=== obstore: downloading {len(keys)} objects into {base} ===")

    if not keys:
        print("  (no keys to download)")
        return

    store = S3Store(
        bucket=S3_TEST_BUCKET,
        prefix="",
        region=AWS_REGION,
    )

    for key in keys:
        if not _is_downloadable_entry(key):
            continue

        local_path = base / key
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            resp = await obs.get_async(store, key)
            with open(local_path, "wb") as f:
                async for chunk in resp:
                    f.write(chunk)
            print(f"  downloaded: {key} -> {local_path}")
        except Exception as e:
            print(f"  FAILED to download {key!r} via obstore:", e)


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
async def main() -> None:
    _require_creds()

    parser = argparse.ArgumentParser(
        description=(
            "Auth + list + download from S3 using aioboto3, aioaws, and obstore.\n"
            "Keys listed by each interface are downloaded into subdirectories of --outdir."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_MAX_ITEMS,
        help=f"Max objects to list/download per interface (default: {DEFAULT_MAX_ITEMS}).",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        required=True,
        help="Base directory where downloaded files will be stored.",
    )
    args = parser.parse_args()

    max_items = args.limit
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    # Auth checks
    await check_aioboto3_sts()
    await check_aioaws_s3()
    await check_obstore_s3()

    # Bucket listings (limited) – collect keys per interface
    aioboto_keys = await list_aioboto3_contents(max_items=max_items)
    aioaws_keys = await list_aioaws_contents(max_items=max_items)
    obstore_keys = await list_obstore_contents(max_items=max_items)

    # Downloads
    await download_aioboto3_files(aioboto_keys, outdir)
    await download_aioaws_files(aioaws_keys, outdir)
    await download_obstore_files(obstore_keys, outdir)


if __name__ == "__main__":
    asyncio.run(main())

