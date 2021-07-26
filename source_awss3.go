package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"net/http"
	"os"
	"strings"
)

type AWSConfig struct {
	S3 AWSS3Config
}

type AWSS3Config struct {
	Buckets []AWSS3Bucket
}

type AWSS3Bucket struct {
	Name   string
	Dist   string
	Prefix string

	EndPoint string
	AppId    string
	AppKey   string
	AppToken string
	Region   string

	EnableLocal bool
	LocalDir    string
}

const AWSS3UrlQueryKey string = "s3"
const ImageSourceAWSS3 ImageSourceType = "s3"

type AWSS3ImageSource struct {
	Config    *SourceConfig
	AWSConfig *AWSConfig
}

func NewAWSS3ImageSource(config *SourceConfig) ImageSource {
	var awsConfig AWSConfig
	if _, err := toml.DecodeFile(config.AWSConfigPath, &awsConfig); err != nil {
		log.Printf("Decode %s error: %s", config.AWSConfigPath, err)
	}
	return &AWSS3ImageSource{config, &awsConfig}
}

func (s *AWSS3ImageSource) Matches(r *http.Request) bool {
	return r.Method == http.MethodGet && s.getAWSS3Param(r) != ""
}

func (s *AWSS3ImageSource) GetImage(r *http.Request) ([]byte, error) {
	path := s.getAWSS3Param(r)
	if path == "" {
		return nil, ErrMissingParamS3
	}
	return s.fetchImage(r)
}

func (s *AWSS3ImageSource) getAWSS3Param(r *http.Request) string {
	return r.URL.Query().Get(AWSS3UrlQueryKey)
}

func (s *AWSS3ImageSource) fetchImage(r *http.Request) ([]byte, error) {
	bConf, key := s.getS3BucketConfig(s.getAWSS3Param(r))
	if bConf == nil {
		return nil, ErrNotFound
	}

	if bConf.EnableLocal {
		localPath := strings.TrimRight(bConf.LocalDir, "/") + "/" + strings.TrimLeft(key, "/")
		f, err := os.OpenFile(localPath, os.O_RDONLY, os.ModePerm)
		if err != nil || f == nil {
			return nil, fmt.Errorf("error open local image: (path=%s) (err=%v)", localPath, err)
		}

		defer f.Close()
		var buf []byte
		if n, err := f.Read(buf); err != nil || n <= 0 {
			return nil, fmt.Errorf("error read local image: (path=%s) (len=%v) (err=%v)", localPath, n, err)
		}
		return buf, nil
	}

	downloader := s.newAWSS3Downloader(bConf)
	buf := &aws.WriteAtBuffer{}

	key = strings.TrimPrefix(key, "/")
	if bConf.Prefix != "" {
		key = strings.Trim(bConf.Prefix, "/") + "/" + key
	}

	_, err := downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bConf.Dist),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("error download aws image: (cfg=%v) (key=%s) (err=%v)", bConf, key, err)
	}

	return buf.Bytes(), nil
}

func (s *AWSS3ImageSource) newAWSS3Downloader(bConf *AWSS3Bucket) *s3manager.Downloader {
	sess := session.Must(session.NewSession(&aws.Config{
		Endpoint:    aws.String(bConf.EndPoint),
		Region:      aws.String(bConf.Region),
		MaxRetries:  aws.Int(2),
		DisableSSL:  aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials: credentials.NewStaticCredentials(bConf.AppId, bConf.AppKey, bConf.AppToken),
	}))
	return s3manager.NewDownloader(sess)
}

func (s *AWSS3ImageSource) getS3BucketConfig(p string) (*AWSS3Bucket, string) {
	pairs := strings.SplitN(p, "/", 2)
	for _, bucket := range s.AWSConfig.S3.Buckets {
		if bucket.Name == pairs[0] {
			return &bucket, pairs[1]
		}
	}
	return nil, ""
}

func init() {
	RegisterSource(ImageSourceAWSS3, NewAWSS3ImageSource)
}
