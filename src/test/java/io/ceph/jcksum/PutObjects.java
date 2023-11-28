/**
 * 
 */
package io.ceph.jcksum;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.*;

import io.ceph.jcksum.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.*; // AttributeMap
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.core.sync.*; // RequestBody
import software.amazon.awssdk.core.checksums.*;
import software.amazon.awssdk.core.checksums.Algorithm;

import org.junit.jupiter.api.*; /* BeforeAll, Test, &c */
import org.junit.jupiter.api.TestInstance.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

/**
 * 
 */
@TestInstance(Lifecycle.PER_CLASS)
class PutObjects {

	public AwsCredentials creds;
	public URI http_uri, ssl_uri;
	static S3Client client, ssl_client;
	
	@BeforeAll
	void setup() throws URISyntaxException {

		creds = AwsBasicCredentials.create(jcksum.access_key, jcksum.secret_key);
		http_uri = new URI(jcksum.http_endpoint);

		SdkHttpClient apacheHttpClient = ApacheHttpClient.builder()
	            .buildWithDefaults(AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build());
		
		/* https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html */
        client = S3Client.builder()
        		.endpointOverride(http_uri)
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(jcksum.region)
                .build();

		ssl_uri = new URI(jcksum.ssl_endpoint);
        ssl_client = S3Client.builder()
        		.httpClient(apacheHttpClient)
        		.endpointOverride(ssl_uri)
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(jcksum.region)
                .build();
		
	}

	/* TODO: zap */
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void testWithExplicitLocalMethodSource(String argument) {
	    assertNotNull(argument);
	    System.out.println("arg: " + argument);
	}

	boolean compareFileDigests(String lhp, String rhp) throws IOException {
		String lh5 = jcksum.getSHA512Sum(lhp);
		String rh5 = jcksum.getSHA512Sum(rhp);
		return lh5.equals(rh5);
	}
	
	boolean putAndVerifyNoCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			PutObjectResponse put_rsp = jcksum.putObjectFromFile(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}
	
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void putObjectFromFileNoCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileNoCksum called with " + in_file_path);
		rslt = putAndVerifyNoCksum(client, in_file_path);
		assertTrue(rslt);
	}
	
	@Test
	void test() {
		boolean rslt = false;
		System.out.println("test");
		rslt = putAndVerifyNoCksum(client, "file-8b");
		assertTrue(rslt);
	}

}
