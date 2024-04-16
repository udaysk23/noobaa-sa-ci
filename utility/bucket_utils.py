"""
Bucket utility functions
"""

import logging
from common_ci_utils.random_utils import (
    generate_random_files,
)
from utility.utils import (
    split_file_data_for_multipart_upload,
)

log = logging.getLogger(__name__)


def upload_incomplete_multipart_object(
    c_scope_s3client,
    tmp_directories_factory,
    amount=1,
):
    """
    Uploads multipart object without actual completing it
    Args:
        c_scope_s3client(Obj): S3 client
        tmp_directories_factory(List): Location of data which needs to be
            uploaded
        amount(int): Object count to be written
    Rerturn:
        Dict: Containing the necessary info

    """
    resp_dir = {}
    origin_dir, results_dir = tmp_directories_factory(
        dirs_to_create=["origin", "result"]
    )
    resp_dir["origin_dir"] = origin_dir
    resp_dir["results_dir"] = results_dir
    # 1. Create a bucket using S3
    bucket_name = c_scope_s3client.create_bucket()
    resp_dir["bucket_name"] = bucket_name
    # 2. Write multipart objects to the bucket
    object_names = generate_random_files(
        origin_dir,
        amount,
        min_size="20M",
        max_size="30M",
    )
    resp_dir["object_names"] = object_names
    # Upload multipart object
    log.info("Initiate multipart upload process")
    for i in range(len(object_names)):
        get_upload_id = c_scope_s3client.initiate_multipart_object_upload(
            bucket_name,
            object_names[i],
        )
        resp_dir[f"{object_names[i]}_upload_id"] = get_upload_id
        all_part_info = []
        file_name = origin_dir + "/" + object_names[i]
        part_size = "10M"
        log.info(f"Split data into {part_size} size")
        part_data = split_file_data_for_multipart_upload(file_name, part_size)
        log.info("Initiate part uploads for multipart object")
        for pd in range(len(part_data)):
            part_id = pd + 1
            part_info = c_scope_s3client.initiate_upload_part(
                bucket_name,
                object_names[i],
                part_id,
                get_upload_id,
                part_data[pd],
            )
            all_part_info.append({"PartNumber": part_id, "ETag": part_info["ETag"]})
        resp_dir["all_part_info"] = all_part_info
    return resp_dir
