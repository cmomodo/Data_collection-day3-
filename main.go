package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

var (
    region         = "us-east-1"
    bucketName     = "sports-analytics-data-lake"
    glueDBName     = "glue_nba_data_lake"
    athenaOutputLoc = "s3://%s/athena-results/"
    apiKey         string
    nbaEndpoint     string
)

func init() {
    // Load environment variables from .env file
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }

    apiKey = os.Getenv("SPORTS_DATA_API_KEY")
    if apiKey == "" {
        log.Fatal("SPORTS_DATA_API_KEY is not set")
    }
    
    nbaEndpoint = os.Getenv("NBA_ENDPOINT")
    if nbaEndpoint == "" {
        log.Fatal("NBA_ENDPOINT is not set")
    }
}

func main() {
    // Load the Shared AWS Configuration (~/.aws/config)
    cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
    if err != nil {
        log.Fatalf("Failed to load configuration: %v", err)
    }

    // Create service clients using the loaded configuration
    s3Client := s3.NewFromConfig(cfg)
    glueClient := glue.NewFromConfig(cfg)
    athenaClient := athena.NewFromConfig(cfg)

    if err := createS3Bucket(s3Client); err != nil {
        log.Fatalf("Failed to create S3 bucket: %v", err)
    }
    time.Sleep(5 * time.Second)
    if err := createGlueDatabase(glueClient); err != nil {
        log.Fatalf("Failed to create Glue database: %v", err)
    }
    nbaData, err := fetchNBAData()
    if err != nil {
        log.Fatalf("Failed to fetch NBA data: %v", err)
    }
    if len(nbaData) > 0 {
        if err := uploadDataToS3(s3Client, bucketName, nbaData); err != nil {
            log.Fatalf("Failed to upload data to S3: %v", err)
        }
    }
    if err := createGlueTable(glueClient); err != nil {
        log.Fatalf("Failed to create Glue table: %v", err)
    }
    if err := configureAthena(athenaClient); err != nil {
        log.Fatalf("Failed to configure Athena: %v", err)
    }
    log.Println("Data lake setup complete.")
}

func createS3Bucket(client *s3.Client) error {
    _, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
        Bucket: aws.String(bucketName),
    })
    return err
}

func createGlueDatabase(client *glue.Client) error {
    _, err := client.CreateDatabase(context.TODO(), &glue.CreateDatabaseInput{
        DatabaseInput: &glue.DatabaseInput{
            Name: aws.String(glueDBName),
        },
    })
    return err
}

func fetchNBAData() ([]map[string]interface{}, error) {
    resp, err := http.Get(nbaEndpoint)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var data []map[string]interface{}
    err = json.NewDecoder(resp.Body).Decode(&data)
    if err != nil {
        return nil, err
    }

    return data, nil
}

func uploadDataToS3(client *s3.Client, bucketName string, data []map[string]interface{}) error {
    jsonData := convertToLineDelimitedJSON(data)
    body := io.NopCloser(strings.NewReader(jsonData))
    _, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
        Bucket: aws.String(bucketName),
        Key:    aws.String("nba_data.json"),
        Body:   body,
    })
    return err
}

func createGlueTable(client *glue.Client) error {
    _, err := client.CreateTable(context.TODO(), &glue.CreateTableInput{
        DatabaseName: aws.String(glueDBName),
        TableInput: &glue.TableInput{
            Name: aws.String("nba_data"),
            StorageDescriptor: &glue.StorageDescriptor{
                Columns: []*glue.Column{
                    {Name: aws.String("id"), Type: aws.String("string")},
                    {Name: aws.String("name"), Type: aws.String("string")},
                    {Name: aws.String("stats"), Type: aws.String("string")},
                },
                Location:     aws.String("s3://" + bucketName + "/"),
                InputFormat:  aws.String("org.apache.hadoop.mapred.TextInputFormat"),
                OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
            },
        },
    })
    return err
}

func configureAthena(client *athena.Client) error {
    _, err := client.StartQueryExecution(context.TODO(), &athena.StartQueryExecutionInput{
        QueryExecutionContext: &athena.QueryExecutionContext{
            Database: aws.String(glueDBName),
        },
        QueryString: aws.String("CREATE DATABASE IF NOT EXISTS " + glueDBName),
        ResultConfiguration: &athena.ResultConfiguration{
            OutputLocation: aws.String(fmt.Sprintf(athenaOutputLoc, bucketName)),
        },
    })
    return err
}

func convertToLineDelimitedJSON(data []map[string]interface{}) string {
    var sb strings.Builder
    for _, item := range data {
        jsonData, _ := json.Marshal(item)
        sb.WriteString(string(jsonData) + "\n")
    }
    return sb.String()
}
