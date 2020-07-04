package com.example.s3.controller;



import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.s3.data.DataObject;
import com.example.s3.service.AwsS3ExampleService;


@RestController
@RequestMapping(value = "/awsexamples3")
public class AwsS3ExampleController {

	@Autowired
	AwsS3ExampleService awsS3ExampleService;
	
	@PostMapping("/addobject")
	public void createObject(@RequestBody DataObject dataObject) throws Exception {
		this.awsS3ExampleService.uploadFile(dataObject);
	}
	
	@GetMapping("/fetchobject/{filename}")
	public void fetchObject(@PathVariable String filename) throws Exception {
		DataObject dataObject = new DataObject();
		dataObject.setName(filename);
		this.awsS3ExampleService.downloadFile(dataObject);
	}
	
	@GetMapping("/listobjects")
	public List<String> listObjects() throws Exception {
		return this.awsS3ExampleService.listObjects();
	}
	
	
	@PutMapping("/updateobject")
	public void updateObject(@RequestBody DataObject dataObject) throws Exception {
		this.awsS3ExampleService.uploadFile(dataObject);
	}
	
	@DeleteMapping("/deleteobject")
	public void deleteObject(@RequestBody DataObject dataObject) {
		this.awsS3ExampleService.deleteFile(dataObject);
	}	
	
	@PostMapping("/addbucket")
	public DataObject createBucket(@RequestBody DataObject dataObject) {
		return this.awsS3ExampleService.addBucket(dataObject);
	}
	
	@GetMapping("/listbuckets")
	public List<String> listBuckets() {
		try {
			return this.awsS3ExampleService.listBuckets();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	@DeleteMapping("/deletebucket") 
	public void deleteBucket(@RequestBody DataObject dataObject) {
		try {
			this.awsS3ExampleService.deleteBucket(dataObject.getName());
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@DeleteMapping("/deletallbuckets")
	public void deleteAllBuckets() {
		try {
			this.awsS3ExampleService.deleteAllBuckets();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
