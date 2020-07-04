package com.example.s3.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import com.example.s3.data.DataObject;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;


@Service
public class AwsS3ExampleService {
	

@Value("${cloud.aws.credentials.accessKey}")
private String key;

@Value("${cloud.aws.credentials.secretKey}")
private String secretKey;

@Value("${cloud.aws.region.static}")
private String region;

@Value("${cloud.aws.bucket.name}")
private String bucketName;
	
private S3AsyncClient s3Client;

@PostConstruct
 public void initialize() {
	AwsCredentials credentials = AwsBasicCredentials.create(key, secretKey);
	AwsCredentialsProvider awsCreds = StaticCredentialsProvider.create(credentials);
	 s3Client = S3AsyncClient.builder()
			 .credentialsProvider(awsCreds)
			 .region(Region.of(region))
			 .build();		 
 }


	public void uploadFile(DataObject dataObject) throws S3Exception, AwsServiceException, 
		SdkClientException, URISyntaxException, FileNotFoundException {

		
			PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).
					key(dataObject.getName()).acl(ObjectCannedACL.PUBLIC_READ).
					build();
			
			
			File file = new File(
					getClass().getClassLoader().getResource(dataObject.getName()).getFile()
				);
			
			s3Client.putObject(putObjectRequest, AsyncRequestBody.fromFile(file));
			
	}
	
	public void downloadFile(DataObject dataObject) throws NoSuchKeyException, S3Exception, AwsServiceException, SdkClientException, IOException
	{
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(dataObject.getName()).build();
		
		s3Client.getObject(getObjectRequest, Paths.get(dataObject.getName()));
		
		
	}
	
	public List<String> listObjects() throws InterruptedException, ExecutionException {
		return this.listObjects(bucketName);
	}
	
	public List<String> listObjects(String name) throws InterruptedException, ExecutionException {
		List<String> names = new ArrayList<>();
		ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(name).build();
		ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest).get();
		listObjectsResponse.contents().stream().forEach(x -> names.add(x.key()));
		return names;
	}
	
	public void deleteFile(DataObject dataObject) {
		this.deleteFile(bucketName, dataObject.getName());		
	}
	
	public void deleteFile(String bucketName, String fileName) {
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(fileName).build();
		s3Client.deleteObject(deleteObjectRequest);
	}
	
	
	public void deleteBucket(String bucket) throws InterruptedException, ExecutionException {
		
		List<String> keys = this.listObjects(bucket);
		List<ObjectIdentifier> identifiers = new ArrayList<>();
		int iteration = 0;
		for(String key : keys) {
			
			ObjectIdentifier objIdentifier = ObjectIdentifier.builder().key(key).build();
		  identifiers.add(objIdentifier);
		  iteration++;
			
			if(iteration == 3){
				iteration = 0;
				DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(identifiers).build()).build();
				s3Client.deleteObjects(deleteObjectsRequest);
				identifiers.clear();
			}

		}
		
		if(identifiers.size() > 0)
		{
			DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(identifiers).build()).build();
			s3Client.deleteObjects(deleteObjectsRequest);

		}
		
		DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
		s3Client.deleteBucket(deleteBucketRequest);
	}
	
	public void deleteAllBuckets() throws InterruptedException, ExecutionException {
		List<String> buckets = this.listBuckets();
		buckets.parallelStream().forEach(x->{
			try {
				this.deleteBucket(x);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}
	
	public DataObject addBucket(DataObject dataObject) {
		dataObject.setName(dataObject.getName() + System.currentTimeMillis());
		CreateBucketRequest createBucketRequest = CreateBucketRequest
		        .builder()
		        .bucket(dataObject.getName()).build();
		        
		s3Client.createBucket(createBucketRequest);
		
		return dataObject;		
		
	}
	
	
	public List<String> listBuckets() throws InterruptedException, ExecutionException{
		List<String> names = new ArrayList<>();
		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest).get();
		listBucketsResponse.buckets().stream().forEach(x -> names.add(x.name()));
		return names;
	}
	
	

}
