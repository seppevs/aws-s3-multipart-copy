'use strict';

const _ = require('lodash');

const {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCopyCommand,
  AbortMultipartUploadCommand,
  ListPartsCommand,
  CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const COPY_PART_SIZE_MINIMUM_BYTES = 5242880; // 5MB in bytes
const DEFAULT_COPY_PART_SIZE_BYTES = 50000000; // 50 MB in bytes
const DEFAULT_COPIED_OBJECT_PERMISSIONS = 'private';

let client;

const init = function (awsS3ClientConfig) {
  client = new S3Client(awsS3ClientConfig);
};

/**
 * Throws the error of initiateMultipartCopy in case such occures
 * @param {*} options an object of parameters obligated to hold the below keys
 * (note that copy_part_size_bytes, copied_object_permissions, expiration_period are optional and will be assigned with default values if not given)
 * @param {*} request_context optional parameter for logging purposes
 */
const copyObjectMultipart = async function ({
                                              source_bucket,
                                              object_key,
                                              destination_bucket,
                                              copied_object_name,
                                              object_size,
                                              copy_part_size_bytes,
                                              copied_object_permissions,
                                              expiration_period,
                                              server_side_encryption,
                                              content_type,
                                              content_disposition,
                                              content_encoding,
                                              content_language,
                                              metadata,
                                              cache_control,
                                              storage_class
                                            }, request_context) {
  const upload_id = await initiateMultipartCopy(destination_bucket, copied_object_name, copied_object_permissions, expiration_period, request_context, server_side_encryption, content_type, content_disposition, content_encoding, content_language, metadata, cache_control, storage_class);
  const partitionsRangeArray = calculatePartitionsRangeArray(object_size, copy_part_size_bytes);
  const copyPartFunctionsArray = [];

  partitionsRangeArray.forEach((partitionRange, index) => {
    copyPartFunctionsArray.push(copyPart(source_bucket, destination_bucket, index + 1, object_key, partitionRange, copied_object_name, upload_id));
  });

  return Promise.all(copyPartFunctionsArray)
    .then((copy_results) => {
      const copyResultsForCopyCompletion = prepareResultsForCopyCompletion(copy_results);
      return completeMultipartCopy(destination_bucket, copyResultsForCopyCompletion, copied_object_name, upload_id);
    })
    .catch(() => {
      return abortMultipartCopy(destination_bucket, copied_object_name, upload_id);
    });
};

async function initiateMultipartCopy(destination_bucket, copied_object_name, copied_object_permissions, expiration_period, request_context, server_side_encryption, content_type, content_disposition, content_encoding, content_language, metadata, cache_control, storage_class) {
  const params = {
    Bucket: destination_bucket,
    Key: copied_object_name,
    ACL: copied_object_permissions || DEFAULT_COPIED_OBJECT_PERMISSIONS
  };
  expiration_period ? params.Expires = expiration_period : null;
  content_type ? params.ContentType = content_type : null;
  content_disposition ? params.ContentDisposition = content_disposition : null;
  content_encoding ? params.ContentEncoding = content_encoding : null;
  content_language ? params.ContentLanguage = content_language : null;
  metadata ? params.Metadata = metadata : null;
  cache_control ? params.CacheControl = cache_control : null;
  server_side_encryption ? params.ServerSideEncryption = server_side_encryption : null;
  storage_class ? params.StorageClass = storage_class : null;

  const command = new CreateMultipartUploadCommand(params);
  const response = await client.send(command);
  return response.UploadId;
}

function copyPart(source_bucket, destination_bucket, part_number, object_key, partition_range, copied_object_name, upload_id) {
  const encodedSourceKey = encodeURIComponent(`${source_bucket}/${object_key}`);
  const params = {
    Bucket: destination_bucket,
    CopySource: encodedSourceKey,
    CopySourceRange: 'bytes=' + partition_range,
    Key: copied_object_name,
    PartNumber: part_number,
    UploadId: upload_id
  };

  const command = new UploadPartCopyCommand(params);
  return client.send(command);
}

function abortMultipartCopy(destination_bucket, copied_object_name, upload_id) {
  const params = {
    Bucket: destination_bucket,
    Key: copied_object_name,
    UploadId: upload_id
  };

  const abortMultipartUploadCommand = new AbortMultipartUploadCommand(params);
  return client.send(abortMultipartUploadCommand)
    .then(() => {
      const listPartsCommand = new ListPartsCommand(params);
      return client.send(listPartsCommand);
    })
    .catch((err) => {
      return Promise.reject(err);
    })
    .then((parts_list) => {
      if (parts_list.Parts.length > 0) {
        const err = new Error('Abort procedure passed but copy parts were not removed');
        err.details = parts_list;
        return Promise.reject(err);
      } else {
        const err = new Error('multipart copy aborted');
        err.details = params;
        return Promise.reject(err);
      }
    });
}

function completeMultipartCopy(destination_bucket, ETags_array, copied_object_name, upload_id) {
  const params = {
    Bucket: destination_bucket,
    Key: copied_object_name,
    MultipartUpload: {
      Parts: ETags_array
    },
    UploadId: upload_id
  };

  const command = new CompleteMultipartUploadCommand(params);
  return client.send(command);
}

function calculatePartitionsRangeArray(object_size, copy_part_size_bytes) {
  const partitions = [];
  const copy_part_size = copy_part_size_bytes || DEFAULT_COPY_PART_SIZE_BYTES;
  const numOfPartitions = Math.floor(object_size / copy_part_size);
  const remainder = object_size % copy_part_size;
  let index, partition;

  for (index = 0; index < numOfPartitions; index++) {
    const nextIndex = index + 1;
    if (nextIndex === numOfPartitions && remainder < COPY_PART_SIZE_MINIMUM_BYTES) {
      partition = (index * copy_part_size) + '-' + ((nextIndex) * copy_part_size + remainder - 1);
    } else {
      partition = (index * copy_part_size) + '-' + ((nextIndex) * copy_part_size - 1);
    }
    partitions.push(partition);
  }

  if (remainder >= COPY_PART_SIZE_MINIMUM_BYTES) {
    partition = (index * copy_part_size) + '-' + (index * copy_part_size + remainder - 1);
    partitions.push(partition);
  }

  return partitions;
}

function prepareResultsForCopyCompletion(copy_parts_results_array) {
  const resultArray = [];

  copy_parts_results_array.forEach((copy_part, index) => {
    const newCopyPart = {};
    newCopyPart.ETag = copy_part.CopyPartResult.ETag;
    newCopyPart.PartNumber = index + 1;
    resultArray.push(newCopyPart);
  });

  return resultArray;
}

module.exports = {
  init,
  copyObjectMultipart,
};
