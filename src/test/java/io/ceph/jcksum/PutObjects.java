/**
 * 
 */
package io.ceph.jcksum;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
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
import org.junit.jupiter.api.Tag;
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

	void generateFile(String in_file_path, String out_file_path, int megs) {
		try {
			File f = new File(in_file_path);
			File of = new File(out_file_path);
			if (of.exists()) {
				of.delete();
			}
			for (int ix = 0; ix < megs; ++ix) {
				InputStream ifs = new FileInputStream(f);
				FileOutputStream ofs = new FileOutputStream(of, true /* append */);
				ifs.transferTo(ofs);
				ofs.close();
				ifs.close();
			}
		} catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
	} /* generateFile */
	
	void generateBigFiles() {
		generateFile("file-1m", "file-5m", 5);
		generateFile("file-1m", "file-10m", 10);
		generateFile("file-1m", "file-100m", 100);
	}
	
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
		
        generateBigFiles();
	} /* setup */

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
	
	boolean putAndVerifyCksum(S3Client s3, String in_file_path) {
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

	boolean putAndVerifyNoCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			PutObjectResponse put_rsp = jcksum.putObjectFromFileNoCksum(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}

	boolean mpuAndVerifyCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			CompleteMultipartUploadResponse put_rsp = jcksum.mpuObjectFromFile(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}

	boolean mpuAndVerifyNoCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			CompleteMultipartUploadResponse put_rsp = jcksum.mpuObjectFromFileNoCksum(s3, in_file_path, out_key_name);
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
	void putObjectFromFileCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileCksum called with " + in_file_path);
		rslt = putAndVerifyCksum(client, in_file_path);
		assertTrue(rslt);
	}
	
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void putObjectFromFileNoCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileNoCksum called with " + in_file_path);
		rslt = putAndVerifyNoCksum(client, in_file_path);
		assertTrue(rslt);
	}

	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#mpuFileNames")
	void mpuObjectFromFileCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("mpuObjectFromFileCksum called with " + in_file_path);
		rslt = mpuAndVerifyCksum(client, in_file_path);
		assertTrue(rslt);
	}

	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#mpuFileNames")
	void mpuObjectFromFileNoCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("mpuObjectFromFileNoCksum called with " + in_file_path);
		rslt = mpuAndVerifyNoCksum(client, in_file_path);
		assertTrue(rslt);
	}
	
	/* SSL */
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void putObjectFromFileCksumSSL(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileCksumSSL called with " + in_file_path);
		rslt = putAndVerifyCksum(ssl_client, in_file_path);
		assertTrue(rslt);
	}
	
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void putObjectFromFileNoCksumSSL(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileNoCksumSSL called with " + in_file_path);
		rslt = putAndVerifyNoCksum(ssl_client, in_file_path);
		assertTrue(rslt);
	}

	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#mpuFileNames")
	void mpuObjectFromFileCksumSSL(String in_file_path) {
		boolean rslt = false;
		System.out.println("mpuObjectFromFileCksumSSL called with " + in_file_path);
		rslt = mpuAndVerifyCksum(ssl_client, in_file_path);
		assertTrue(rslt);
	}

	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#mpuFileNames")
	void mpuObjectFromFileNoCksumSSL(String in_file_path) {
		boolean rslt = false;
		System.out.println("mpuObjectFromFileNoCksumSSL called with " + in_file_path);
		rslt = mpuAndVerifyNoCksum(ssl_client, in_file_path);
		assertTrue(rslt);
	}

} /* class PutObjects */
