package outputs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/falcosecurity/falcosidekick/types"
	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/objectstorage"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

// NewOCIClient returns a new output.Client for accessing the OCI API.
func NewOCIClient(config *types.Configuration, stats *types.Statistics, promStats *types.PromStatistics, statsdClient, dogstatsdClient *statsd.Client) (*Client, error) {
	return &Client{
		OutputType:      "OCI",
		Config:          config,
		Stats:           stats,
		PromStats:       promStats,
		StatsdClient:    statsdClient,
		DogstatsdClient: dogstatsdClient,
	}, nil
}

func (c *Client) UploadToObjectStorage(falcopayload types.FalcoPayload) {
	f, _ := json.Marshal(falcopayload)

	prefix := ""
	t := time.Now()
	if c.Config.OCI.ObjectStorage.ObjectNamePrefix != "" {
		prefix = strings.TrimSpace(c.Config.OCI.ObjectStorage.ObjectNamePrefix)
	}

	objectName := fmt.Sprintf("%s/%s/%s.json", prefix, t.Format("2006-01-02"), t.Format(time.RFC3339Nano))

	request := objectstorage.PutObjectRequest{
		BucketName:    common.String(strings.TrimSpace(c.Config.OCI.ObjectStorage.Bucket)),
		NamespaceName: common.String(strings.TrimSpace(c.Config.OCI.ObjectStorage.Namespace)),
		ObjectName:    common.String(strings.TrimSpace(objectName)),
		PutObjectBody: io.NopCloser(bytes.NewReader(f)),
		ContentLength: common.Int64(int64(bytes.NewReader(f).Len())),
	}

	privateKeyBytes, err := os.ReadFile(strings.TrimSpace(c.Config.OCI.PrivateKey))
	if err != nil {
		log.Printf("Error while reading Private Key from file [%v]", err.Error())
	}
	provider := common.NewRawConfigurationProvider(strings.TrimSpace(c.Config.OCI.Tenancy), strings.TrimSpace(c.Config.OCI.User), strings.TrimSpace(c.Config.OCI.Region), strings.TrimSpace(c.Config.OCI.Fingerprint),
		string(privateKeyBytes), common.String(c.Config.OCI.Passphrase))

	objStorageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		log.Printf("Error while creating Object Storage Client [%v]", err.Error())
	}

	_, err = objStorageClient.PutObject(context.Background(), request)

	if err != nil {
		log.Printf("[ERROR] : OCIObjectStorage - %v - %v\n", "Error while Uploading message", err.Error())
		c.Stats.OCIObjectStorage.Add(Error, 1)
		go c.CountMetric("outputs", 1, []string{"output:ociobjectstorage", "status:error"})
		c.PromStats.Outputs.With(map[string]string{"destination": "ociobjectstorage", "status": Error}).Inc()
		return
	}

	log.Printf("[INFO]  : OCIObjectStorage - Upload to bucket OK \n")
	c.Stats.GCPStorage.Add(OK, 1)
	go c.CountMetric("outputs", 1, []string{"output:ociobjectstorage", "status:ok"})
	c.PromStats.Outputs.With(map[string]string{"destination": "ociobjectstorage", "status": OK}).Inc()
}
