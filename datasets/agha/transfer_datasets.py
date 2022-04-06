"""
Transfer datasets from presigned URLs to a dataset's GCP main-upload bucket.
"""
from typing import Optional
import os
from shlex import quote

import click
import hailtop.batch as hb
from cloudpathlib import AnyPath

DRIVER_IMAGE = os.getenv("CPG_DRIVER_IMAGE")
DATASET = os.getenv("CPG_DATASET")

assert DRIVER_IMAGE and DATASET


@click.command("Transfer_datasets from signed URLs")
@click.option("--presigned-url-file-path", multiple=True)
@click.option("--batch-size", multiple=True)
def main(
    presigned_url_file_path: str,
    batch_size: int = 5,
    subfolder: Optional[str] = None,
):
    """
    Given a list of presigned URLs, download the files and upload them to GCS.
    """

    with open(AnyPath(presigned_url_file_path)) as file:
        presigned_urls = [l.strip() for l in file.readlines() if l.strip()]

    incorrect_urls = [url for url in presigned_urls if not url.startswith("https://")]
    if incorrect_urls:
        raise Exception(f"Incorrect URLs: {incorrect_urls}")

    batch = hb.Batch(f"transfer {DATASET}", default_image=DRIVER_IMAGE)

    output_path = f"gs://cpg-{DATASET}-main-upload/{subfolder}"

    # may as well batch them to reduce the number of VMs
    for idx in range(len(presigned_urls) // batch_size):
        batched_urls = presigned_urls[idx * batch_size : (idx + 1) * batch_size]

        j = batch.new_job(f"batch {idx} (size={len(batched_urls)}")
        for url in batched_urls:
            filename = os.path.basename(url).split("?")[0]
            quoted_url = quote(url)
            j.command(
                f"curl -L {quoted_url} | gsutil cp - {os.path.join(output_path, filename)}"
            )

    # batch.run(wait=False)
    batch.run(dry_run=True)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
