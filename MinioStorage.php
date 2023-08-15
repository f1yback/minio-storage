<?php


namespace app\components;

use Aws\S3\Exception\S3Exception;
use Psr\Log\LogLevel;

/**
 * MinioStorage component
 */
class MinioStorage
{

    protected $minio;

    public function __construct()
    {
        $credentials = new \Aws\Credentials\Credentials(\Yii::$app->params['minio_user'], \Yii::$app->params['minio_secret']);
        $this->minio = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region'  => 'us-east-1',
            'endpoint' => \Yii::$app->params['minio_server'],
            'use_path_style_endpoint' => true,
            'credentials' => $credentials,
        ]);
    }

    /**
     * @param string|null $filename
     * @param string $fileSource
     * @param string $bucket
     * @return \Aws\Result|void
     */
    public function upload(?string $filename, string $fileSource, string $bucket = 'public')
    {
        if (!$this->minio->doesBucketExist($bucket)){
            $this->minio->createBucket([
                'ACL' => 'public-read',
                'Bucket' => $bucket,
                'CreateBucketConfiguration' => [
                    'LocationConstraint' => 'us-east-1',
                ],
                'ObjectLockEnabledForBucket' => true,
            ]);
        }
        try {
            $res = $this->minio->putObject([
                'Bucket' => $bucket,
                'Key'    => $filename,
                'SourceFile' => $fileSource,
            ]);
        } catch (S3Exception $exception){
            print_r($exception->getMessage());
            die();
        }
        return $res;
    }

    /**
     * @param string|null $filename
     * @param string $bucket
     * @return string
     */
    public function getFile(?string $filename = null, string $bucket = 'public'): string
    {
        if (!$filename) {
            return '';
        }
        return $this->minio->getObjectUrl($bucket, $filename);
    }

    /**
     * @param string $bucket
     * @return \Aws\Result
     */
    public function getAll(string $bucket = 'public'): \Aws\Result
    {
        return $this->minio->listObjectsV2([
            'Bucket' => $bucket,
        ]);
    }

    /**
     * @param string|null $filename
     * @param string $bucket
     * @return string
     */
    public function getObject(?string $filename = null, string $bucket = 'public'): string
    {
        if (!$filename) {
            return '';
        }
        $object = $this->minio->getObject([
            'Bucket' => $bucket,
            'Key'    => $filename
        ]);
        $data = '';
        while ($d = $object['Body']->read(1024)) {
            $data .= $d;
        }
        return  $data;
    }

    /**
     * @param string|null $filename
     * @param string $bucket
     * @return void
     */
    public function deleteObject(?string $filename = null, string $bucket = 'public')
    {
        if (!$filename) {
            return;
        }
        if ($this->doesObjectExist($filename, $bucket)) {
            try {
                $this->minio->deleteObject([
                    'Bucket' => $bucket,
                    'Key'    => $filename
                ]);
            } catch (S3Exception $e) {
                \Yii::getLogger()->log($e->getMessage(), LogLevel::ALERT);
                die();
            }
        }
    }

    /**
     * @param string|null $filename
     * @param string $bucket
     * @param string|null $original
     * @return bool|string
     */
    public function doesObjectExist(?string $filename = null, string $bucket = 'public', ?string $original = null)
    {
        if (!$filename) {
            return '';
        }
        if (!empty($original)) {
            $filename = $original;
        }
        return $this->minio->doesObjectExist($bucket, $filename);
    }

    /**
     * @param string $key
     * @param string $copysource
     * @param string $bucket
     * @param string|null $original
     * @return \Aws\Result|false|void
     */
    public function copy(string $key, string $copysource, string $bucket = 'public', ?string $original = null)
    {
        if ($this->doesObjectExist($copysource, 'public', $original)) {
            try {
                $res = $this->minio->copyObject([
                    'Bucket' => $bucket,
                    'Key'    => $key,
                    'CopySource' => $copysource,
                ]);
            } catch (S3Exception $exception){
                print_r($exception->getMessage());
                die();
            }
            return $res;
        }
        return false;
    }
}
