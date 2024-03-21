/// <reference types="node" />
import stream from 'node:stream';
import { S3ClientConfig } from '@aws-sdk/client-s3';
import { DataStore, Upload, KvStore } from '@tus/utils';
type Options = {
    partSize?: number;
    useTags?: boolean;
    maxConcurrentPartUploads?: number;
    cache?: KvStore<MetadataValue>;
    expirationPeriodInMilliseconds?: number;
    s3ClientConfig: S3ClientConfig & {
        bucket: string;
    };
};
export type MetadataValue = {
    file: Upload;
    'upload-id': string;
    'tus-version': string;
};
export declare class S3Store extends DataStore {
    private bucket;
    private cache;
    private client;
    private preferredPartSize;
    private expirationPeriodInMilliseconds;
    private useTags;
    private partUploadSemaphore;
    maxMultipartParts: 10000;
    minPartSize: 5242880;
    maxUploadSize: 5497558138880;
    constructor(options: Options);
    protected shouldUseExpirationTags(): boolean;
    protected useCompleteTag(value: 'true' | 'false'): string | undefined;
    /**
     * Saves upload metadata to a `${file_id}.info` file on S3.
     * Please note that the file is empty and the metadata is saved
     * on the S3 object's `Metadata` field, so that only a `headObject`
     * is necessary to retrieve the data.
     */
    private saveMetadata;
    private completeMetadata;
    /**
     * Retrieves upload metadata previously saved in `${file_id}.info`.
     * There's a small and simple caching mechanism to avoid multiple
     * HTTP calls to S3.
     */
    private getMetadata;
    private infoKey;
    private partKey;
    private uploadPart;
    private uploadIncompletePart;
    private downloadIncompletePart;
    private getIncompletePart;
    private getIncompletePartSize;
    private deleteIncompletePart;
    /**
     * Uploads a stream to s3 using multiple parts
     */
    private uploadParts;
    /**
     * Completes a multipart upload on S3.
     * This is where S3 concatenates all the uploaded parts.
     */
    private finishMultipartUpload;
    /**
     * Gets the number of complete parts/chunks already uploaded to S3.
     * Retrieves only consecutive parts.
     */
    private retrieveParts;
    /**
     * Removes cached data for a given file.
     */
    private clearCache;
    private calcOptimalPartSize;
    /**
     * Creates a multipart upload on S3 attaching any metadata to it.
     * Also, a `${file_id}.info` file is created which holds some information
     * about the upload itself like: `upload-id`, `upload-length`, etc.
     */
    create(upload: Upload): Promise<Upload>;
    read(id: string): Promise<stream.Readable>;
    /**
     * Write to the file, starting at the provided offset
     */
    write(src: stream.Readable, id: string, offset: number): Promise<number>;
    getUpload(id: string): Promise<Upload>;
    declareUploadLength(file_id: string, upload_length: number): Promise<void>;
    remove(id: string): Promise<void>;
    protected getExpirationDate(created_at: string): Date;
    getExpiration(): number;
    deleteExpired(): Promise<number>;
    private uniqueTmpFileName;
}
export {};
