package main

import (
	"context"
	"encoding/json"
	"errors"
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
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
)

var (
	region          = "us-east-1"
	bucketName      = "sports-analytics-data-lake3"
	glueDBName      = "glue_nba_data_lake"
	athenaOutputLoc = "s3://%s/athena-results/"
	apiKey          string
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
	// Check if the bucket exists
	_, err := client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		log.Printf("Bucket %s already exists", bucketName)
		return nil
	}

	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		// Bucket does not exist, create it
		_, err = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		log.Printf("Bucket %s created successfully", bucketName)
		return nil
	}

	// Return any other error
	return fmt.Errorf("failed to check if bucket exists: %w", err)
}

func createGlueDatabase(client *glue.Client) error {
	_, err := client.CreateDatabase(context.TODO(), &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{
			Name: aws.String(glueDBName),
		},
	})
	return err
}
func fetchNBAData() ([]map[string]interface{}, error) {
    // Create a new HTTP client and request
    client := &http.Client{}
    req, err := http.NewRequest("GET", nbaEndpoint, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create request: %w", err)
    }

    // Inject API key from .env into headers (secure)
    req.Header.Add("Ocp-Apim-Subscription-Key", apiKey) // Adjust header format as per API docs

    // Execute request
    resp, err := client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("API request failed: %w", err)
    }
    defer resp.Body.Close()

    // Check for HTTP errors (e.g., 401 Unauthorized)
    if resp.StatusCode != http.StatusOK {
        bodyBytes, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("API returned %d: %s", resp.StatusCode, string(bodyBytes))
    }

    var result interface{}
    err = json.NewDecoder(resp.Body).Decode(&result)
    if err != nil {
        return nil, err
    }

    switch data := result.(type) {
    case []interface{}:
        var nbaData []map[string]interface{}
        for _, item := range data {
            nbaData = append(nbaData, item.(map[string]interface{}))
        }
        return nbaData, nil
    case map[string]interface{}:
        return []map[string]interface{}{data}, nil
    default:
        return nil, fmt.Errorf("unexpected JSON structure")
    }
}

func uploadDataToS3(client *s3.Client, bucketName string, data []map[string]interface{}) error {
    jsonData := convertToLineDelimitedJSON(data)
    contentLength := int64(len(jsonData))
    body := io.NopCloser(strings.NewReader(jsonData))

    _, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
        Bucket:        aws.String(bucketName),
        Key:           aws.String("nba_data.json"),
        Body:          body,
        ContentLength: &contentLength, // Add Content-Length header
    })
    return err
}

func createGlueTable(client *glue.Client) error {
	_, err := client.CreateTable(context.TODO(), &glue.CreateTableInput{
		DatabaseName: aws.String(glueDBName),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("nba_data"),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Columns: []gluetypes.Column{
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
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(glueDBName),
		},
		QueryString: aws.String("CREATE DATABASE IF NOT EXISTS " + glueDBName),
		ResultConfiguration: &athenatypes.ResultConfiguration{
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