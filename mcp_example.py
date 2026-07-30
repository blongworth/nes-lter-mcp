import asyncio
import io
import os
import tarfile
import tempfile

import httpx
import yaml
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("OpenDataMCP")

# Use the Git Trees API with recursive flag — returns ALL files in one request,
# no pagination limits like the Contents API (which caps at 1000).
TREE_API_URL = "https://api.github.com/repos/awslabs/open-data-registry/git/trees/main?recursive=1"
RAW_BASE_URL = "https://raw.githubusercontent.com/awslabs/open-data-registry/main/datasets"
TAGS_URL = "https://raw.githubusercontent.com/awslabs/open-data-registry/main/tags.yaml"
TARBALL_URL = "https://api.github.com/repos/awslabs/open-data-registry/tarball/main"


async def _fetch_and_parse(client: httpx.AsyncClient, filename: str) -> dict | None:
    """Fetch a single YAML file and parse it. Returns None on failure."""
    raw_url = f"{RAW_BASE_URL}/{filename}"
    try:
        response = await client.get(raw_url)
        response.raise_for_status()
        return yaml.safe_load(response.text)
    except Exception:
        return None


async def _fetch_all_datasets(client: httpx.AsyncClient) -> list[tuple[str, dict]]:
    """Download the repo tarball and parse all dataset YAMLs from it.

    Returns a list of (slug, parsed_yaml_dict) tuples.
    """
    response = await client.get(
        TARBALL_URL,
        headers={"Accept": "application/vnd.github.v3+json"},
        follow_redirects=True,
    )
    response.raise_for_status()

    datasets = []
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as tar:
        for member in tar.getmembers():
            # Tarball paths look like: awslabs-open-data-registry-<sha>/datasets/<name>.yaml
            parts = member.name.split("/")
            if (
                len(parts) >= 3
                and parts[1] == "datasets"
                and parts[-1].endswith(".yaml")
                and member.isfile()
            ):
                f = tar.extractfile(member)
                if f is None:
                    continue
                try:
                    data = yaml.safe_load(f.read())
                    if data and isinstance(data, dict):
                        slug = parts[-1].removesuffix(".yaml")
                        datasets.append((slug, data))
                except Exception:
                    continue
    return datasets


@mcp.tool()
async def search_datasets(query: str) -> list[dict]:
    """Search AWS Open Data Registry datasets by name.

    Args:
        query: Search term to match against dataset names (case-insensitive).

    Returns:
        A list of matching datasets with their metadata (name, description,
        tags, resources, documentation URL, etc.).
    """
    query_lower = query.lower()

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Single request to get the full repo tree (all files, no cap)
        response = await client.get(
            TREE_API_URL,
            headers={"Accept": "application/vnd.github.v3+json"},
        )
        response.raise_for_status()
        tree = response.json().get("tree", [])

        # Filter to dataset YAML files whose filename matches the query
        candidates = [
            entry["path"].removeprefix("datasets/")
            for entry in tree
            if entry["path"].startswith("datasets/")
            and entry["path"].endswith(".yaml")
            and query_lower in entry["path"].lower()
        ]

        # Fetch all matching YAMLs in parallel
        tasks = [_fetch_and_parse(client, name) for name in candidates]
        parsed = await asyncio.gather(*tasks)

    # Build results from successfully parsed files
    results = []
    for filename, dataset in zip(candidates, parsed):
        if dataset is None:
            continue
        name = dataset.get("Name", "")
        if query_lower in name.lower() or query_lower in filename.lower():
            slug = filename.removesuffix(".yaml")
            results.append({
                "slug": slug,
                "name": name,
                "description": dataset.get("Description", ""),
                "documentation": dataset.get("Documentation", ""),
                "contact": dataset.get("Contact", ""),
                "managed_by": dataset.get("ManagedBy", ""),
                "update_frequency": dataset.get("UpdateFrequency", ""),
                "tags": dataset.get("Tags", []),
                "license": dataset.get("License", ""),
                "resources": [
                    {
                        "description": r.get("Description", ""),
                        "arn": r.get("ARN", ""),
                        "region": r.get("Region", ""),
                        "type": r.get("Type", ""),
                    }
                    for r in dataset.get("Resources", [])
                ],
            })

    return results


@mcp.tool()
async def get_dataset(slug: str) -> dict:
    """Get full details for a specific AWS Open Data Registry dataset.

    Args:
        slug: The dataset identifier (YAML filename without .yaml extension),
              e.g. "sentinel-2", "noaa-goes", "nasa-nex". You can discover
              valid slugs from the search_datasets tool results.

    Returns:
        Complete dataset metadata including name, description, tags,
        resources, license, documentation, and data-at-work references
        (tutorials, tools, publications).
    """
    filename = f"{slug}.yaml"
    async with httpx.AsyncClient(timeout=30.0) as client:
        dataset = await _fetch_and_parse(client, filename)

    if dataset is None:
        return {"error": f"Dataset '{slug}' not found. Use search_datasets to find valid slugs."}

    return {
        "name": dataset.get("Name", ""),
        "description": dataset.get("Description", ""),
        "documentation": dataset.get("Documentation", ""),
        "contact": dataset.get("Contact", ""),
        "managed_by": dataset.get("ManagedBy", ""),
        "update_frequency": dataset.get("UpdateFrequency", ""),
        "tags": dataset.get("Tags", []),
        "license": dataset.get("License", ""),
        "resources": [
            {
                "description": r.get("Description", ""),
                "arn": r.get("ARN", ""),
                "region": r.get("Region", ""),
                "type": r.get("Type", ""),
                "controlled_access": r.get("ControlledAccess", ""),
            }
            for r in dataset.get("Resources", [])
        ],
        "data_at_work": {
            "tutorials": [
                {
                    "title": t.get("Title", ""),
                    "url": t.get("URL", ""),
                    "author_name": t.get("AuthorName", ""),
                    "author_url": t.get("AuthorURL", ""),
                }
                for t in dataset.get("DataAtWork", {}).get("Tutorials", []) or []
            ],
            "tools_and_applications": [
                {
                    "title": t.get("Title", ""),
                    "url": t.get("URL", ""),
                    "author_name": t.get("AuthorName", ""),
                    "author_url": t.get("AuthorURL", ""),
                }
                for t in dataset.get("DataAtWork", {}).get("Tools & Applications", []) or []
            ],
            "publications": [
                {
                    "title": p.get("Title", ""),
                    "url": p.get("URL", ""),
                    "author_name": p.get("AuthorName", ""),
                    "author_url": p.get("AuthorURL", ""),
                }
                for p in dataset.get("DataAtWork", {}).get("Publications", []) or []
            ],
        },
    }


@mcp.tool()
async def list_tags() -> list[str]:
    """List all valid tags in the AWS Open Data Registry.

    Returns:
        A sorted list of valid tag strings that can be used with
        search_datasets_by_tags.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(TAGS_URL)
        response.raise_for_status()
        tags = yaml.safe_load(response.text)
    return sorted(tags, key=str.lower)


@mcp.tool()
async def search_datasets_by_tags(tags: list[str]) -> dict:
    """Search AWS Open Data Registry datasets by one or more tags.

    Downloads the full registry and filters datasets that contain ALL of the
    specified tags. Tags are matched case-insensitively.

    Args:
        tags: One or more tags to filter by (e.g. ["climate", "netcdf"]).
              Valid tags can be found using the list_tags tool.

    Returns:
        A dict with the count of matching datasets and a list of results,
        each containing the slug (for use with get_dataset) and name.
    """
    if not tags:
        return {"error": "Please provide at least one tag."}

    tags_lower = {t.lower() for t in tags}

    async with httpx.AsyncClient(timeout=60.0) as client:
        all_datasets = await _fetch_all_datasets(client)

    results = []
    for slug, dataset in all_datasets:
        dataset_tags = {t.lower() for t in dataset.get("Tags", [])}
        if tags_lower.issubset(dataset_tags):
            results.append({
                "slug": slug,
                "name": dataset.get("Name", ""),
            })

    return {
        "count": len(results),
        "datasets": results,
    }


@mcp.tool()
async def search_stac_endpoints(query: str = "") -> dict:
    """Search for STAC (SpatioTemporal Asset Catalog) endpoints in the registry.

    Scans all dataset YAMLs for STAC API endpoints listed in resource
    Explore links. Optionally filters by a search term against dataset name.

    Args:
        query: Optional search term to filter datasets by name
               (case-insensitive). Leave empty to return all STAC endpoints.

    Returns:
        A dict with the count of datasets with STAC endpoints and a list
        of results, each containing slug, name, and stac_endpoints (URLs).
    """
    import re

    query_lower = query.lower()

    async with httpx.AsyncClient(timeout=60.0) as client:
        all_datasets = await _fetch_all_datasets(client)

    # Regex to extract URLs from markdown links like [text](url)
    link_pattern = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")

    results = []
    for slug, dataset in all_datasets:
        name = dataset.get("Name", "")

        # Apply optional name filter
        if query_lower and query_lower not in name.lower() and query_lower not in slug.lower():
            continue

        stac_endpoints = []
        for resource in dataset.get("Resources", []):
            for explore_item in resource.get("Explore", []) or []:
                # Check if this explore item mentions STAC
                if "stac" not in explore_item.lower():
                    continue
                # Extract URL from markdown link
                match = link_pattern.search(explore_item)
                if match:
                    label = match.group(1)
                    url = match.group(2)
                    stac_endpoints.append({"label": label, "url": url})
                else:
                    # Plain URL or text
                    stac_endpoints.append({"label": explore_item, "url": explore_item})

        if stac_endpoints:
            results.append({
                "slug": slug,
                "name": name,
                "stac_endpoints": stac_endpoints,
            })

    return {
        "count": len(results),
        "datasets": results,
    }


@mcp.tool()
async def query_stac_items(
    collection_url: str,
    bbox: str = "",
    datetime_range: str = "",
    limit: int = 10,
) -> dict:
    """Query a STAC collection endpoint for available items (images/assets).

    Given a STAC collection URL, fetches the items available in that
    collection with optional spatial and temporal filters.

    Args:
        collection_url: The STAC collection URL, e.g.
            "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
        bbox: Optional bounding box as "west,south,east,north" in WGS84
              decimal degrees, e.g. "10.0,50.0,11.0,51.0"
        datetime_range: Optional datetime filter in RFC 3339 format.
              Single date: "2024-01-01T00:00:00Z"
              Range: "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z"
              Open range: "../2024-01-31T23:59:59Z" or "2024-01-01T00:00:00Z/.."
        limit: Maximum number of items to return (default 10, max 100).

    Returns:
        A dict with collection info, the number of items returned,
        and a list of items with id, datetime, bbox, geometry, assets,
        and properties.
    """
    limit = min(max(1, limit), 100)

    # Build the items URL from the collection URL
    items_url = collection_url.rstrip("/") + "/items"

    params: dict = {"limit": limit}
    if bbox:
        params["bbox"] = bbox
    if datetime_range:
        params["datetime"] = datetime_range

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(items_url, params=params)
        response.raise_for_status()
        data = response.json()

    features = data.get("features", [])

    items = []
    for feature in features:
        properties = feature.get("properties", {})
        assets = feature.get("assets", {})
        items.append({
            "id": feature.get("id", ""),
            "datetime": properties.get("datetime", ""),
            "bbox": feature.get("bbox", []),
            "cloud_cover": properties.get("eo:cloud_cover"),
            "asset_keys": list(assets.keys()),
            "properties": {
                k: v for k, v in properties.items()
                if k in (
                    "datetime", "platform", "constellation",
                    "instruments", "gsd", "eo:cloud_cover",
                    "proj:epsg", "created", "updated",
                )
            },
        })

    return {
        "collection_url": collection_url,
        "items_returned": len(items),
        "items": items,
    }


@mcp.tool()
async def get_stac_item_thumbnail(
    item_id: str,
    collection_url: str = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
) -> dict:
    """Get the thumbnail image URL and metadata for a STAC item (scene).

    Fetches a specific item from a STAC collection and returns its
    thumbnail URL along with key scene metadata.

    Args:
        item_id: The STAC item identifier, e.g. "S2A_19TCG_20241219_0_L2A".
        collection_url: The STAC collection URL. Defaults to the Earth Search
            Sentinel-2 L2A collection.

    Returns:
        A dict with the item id, thumbnail URL, datetime, cloud cover,
        bbox, platform, and a list of all available asset keys.
    """
    item_url = collection_url.rstrip("/") + f"/items/{item_id}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(item_url)
        if response.status_code == 404:
            return {"error": f"Item '{item_id}' not found at {collection_url}"}
        response.raise_for_status()
        data = response.json()

    properties = data.get("properties", {})
    assets = data.get("assets", {})

    # Look for thumbnail asset
    thumbnail = assets.get("thumbnail", {})
    thumbnail_url = thumbnail.get("href", "")

    # If no thumbnail, try rendered_preview or visual
    if not thumbnail_url:
        for key in ("rendered_preview", "visual"):
            if key in assets:
                thumbnail_url = assets[key].get("href", "")
                break

    return {
        "item_id": data.get("id", item_id),
        "collection": data.get("collection", ""),
        "thumbnail_url": thumbnail_url,
        "thumbnail_type": thumbnail.get("type", ""),
        "datetime": properties.get("datetime", ""),
        "cloud_cover": properties.get("eo:cloud_cover"),
        "bbox": data.get("bbox", []),
        "platform": properties.get("platform", ""),
        "constellation": properties.get("constellation", ""),
        "instruments": properties.get("instruments", []),
        "epsg": properties.get("proj:epsg"),
        "asset_keys": list(assets.keys()),
    }


@mcp.tool()
async def compare_images(
    item_id_before: str,
    item_id_after: str,
    collection_url: str = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
    title_before: str = "Before",
    title_after: str = "After",
    output_path: str = "",
) -> dict:
    """Create a side-by-side comparison plot of two satellite images from STAC items.

    Fetches the visual/RGB asset from two STAC items and creates a matplotlib
    side-by-side subplot comparison (useful for before/after analysis such as
    deforestation, urbanization, or natural disasters).

    Args:
        item_id_before: STAC item ID for the "before" image (left panel).
        item_id_after: STAC item ID for the "after" image (right panel).
        collection_url: Full URL to the STAC collection (defaults to Earth Search Sentinel-2 L2A).
        title_before: Title for the left panel (default: "Before").
        title_after: Title for the right panel (default: "After").
        output_path: Path to save the output PNG. If empty, saves to a temp file.

    Returns:
        A dict with the output file path and metadata for both images.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # Non-interactive backend
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return {"error": "matplotlib and numpy are required. Install with: pip install matplotlib numpy"}

    async def fetch_item(item_id: str) -> dict:
        """Fetch STAC item JSON."""
        url = collection_url.rstrip("/") + f"/items/{item_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()

    async def download_image(url: str) -> bytes:
        """Download image bytes from a URL."""
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content

    def get_visual_url(item: dict) -> str | None:
        """Extract the best visual asset URL from a STAC item."""
        assets = item.get("assets", {})
        for key in ("thumbnail", "visual", "true_color", "rendered_preview"):
            if key in assets:
                return assets[key].get("href")
        return None

    # Fetch both items in parallel
    item_before, item_after = await asyncio.gather(
        fetch_item(item_id_before),
        fetch_item(item_id_after),
    )

    url_before = get_visual_url(item_before)
    url_after = get_visual_url(item_after)

    if not url_before:
        return {
            "error": f"No visual asset found for item '{item_id_before}'.",
            "available_assets": list(item_before.get("assets", {}).keys()),
        }
    if not url_after:
        return {
            "error": f"No visual asset found for item '{item_id_after}'.",
            "available_assets": list(item_after.get("assets", {}).keys()),
        }

    # Download both images in parallel
    img_bytes_before, img_bytes_after = await asyncio.gather(
        download_image(url_before),
        download_image(url_after),
    )

    # Decode images
    try:
        from PIL import Image
        arr_before = np.array(Image.open(io.BytesIO(img_bytes_before)))
        arr_after = np.array(Image.open(io.BytesIO(img_bytes_after)))
    except ImportError:
        from matplotlib import image as mpimg
        arr_before = mpimg.imread(io.BytesIO(img_bytes_before), format="png")
        arr_after = mpimg.imread(io.BytesIO(img_bytes_after), format="png")

    # Create side-by-side plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))

    ax1.imshow(arr_before)
    ax1.set_title(title_before, fontsize=16, fontweight="bold")
    ax1.axis("off")

    ax2.imshow(arr_after)
    ax2.set_title(title_after, fontsize=16, fontweight="bold")
    ax2.axis("off")

    plt.tight_layout()

    # Determine output path
    if not output_path:
        output_path = os.path.join(
            tempfile.gettempdir(),
            f"compare_{item_id_before}_vs_{item_id_after}.png",
        )

    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Gather metadata
    props_before = item_before.get("properties", {})
    props_after = item_after.get("properties", {})

    return {
        "output_path": output_path,
        "before": {
            "item_id": item_id_before,
            "datetime": props_before.get("datetime"),
            "cloud_cover": props_before.get("eo:cloud_cover"),
            "image_url": url_before,
        },
        "after": {
            "item_id": item_id_after,
            "datetime": props_after.get("datetime"),
            "cloud_cover": props_after.get("eo:cloud_cover"),
            "image_url": url_after,
        },
        "message": f"Side-by-side comparison saved to {output_path}",
    }


def main():
    # Initialize and run the server
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
 
