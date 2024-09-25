export type Part = {
  ETag: string;
  PartNumber: number;
};

export type PayloadData = {
  Body: Buffer;
  Bucket: string;
  Key: string;
  PartNumber: number;
  UploadId: string;
  ContentMD5: string;
};
