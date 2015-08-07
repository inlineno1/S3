import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.imgscalr.Scalr;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

public class S3ImageNameChange {

	public static final String BUCKET_NAME = "com.ma2rix.upload.test";

	public static final Regions REGION = Regions.US_EAST_1;
	
	public static final String [] SUFFIXS = {"png", "jpg", "jpeg", "gif", "bmp"};

	public static void main(String[] args) {

		// S3 clinet authentication
		AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region region = Region.getRegion(REGION);
		s3.setRegion(region);
		
		System.out.println("===========================================");
		System.out.println(" Start ...");
		System.out.println("===========================================\n");

		try {
			ObjectListing objectList = s3.listObjects(BUCKET_NAME);

			boolean objectAddCheck = false;
			do {
				// next object add
				if (objectAddCheck)
					objectList = s3.listNextBatchOfObjects(objectList);

				for (S3ObjectSummary s3Object : objectList.getObjectSummaries()) {
					String objectKey = s3Object.getKey();

					int pos = 0;
					int pathPos = 0;
					int tumbPos = 0;
					
					for (String suffix : SUFFIXS) {
					
						pos = objectKey.toLowerCase().lastIndexOf(suffix);
						pathPos = objectKey.lastIndexOf("Image");
						tumbPos = objectKey.lastIndexOf("_tumb");
						
						if (pos != -1 && pathPos != -1 && tumbPos == -1) {
							S3Object object = s3.getObject(BUCKET_NAME,objectKey);
							InputStream in = object.getObjectContent();
							BufferedImage src = ImageIO.read(in);

							// image width : 150px
							imageResize(s3, credentials, objectKey, pos, suffix, src, 150);
							
							// image width : 40px
							imageResize(s3, credentials, objectKey, pos, suffix, src, 40);
							
							break;
						}
					}
				}
				objectAddCheck = true;

			} while (objectList.isTruncated());

		} catch (AmazonServiceException ase) {
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Error Message: " + ace.getMessage());
		} catch (IOException ioe) {
			System.out.println("Error Message: " + ioe.getMessage());
		} finally {
			System.out.println("\n");
			System.out.println("===========================================");
			System.out.println(" End ...");
			System.out.println("===========================================\n");
		}
	}
	
	private static void imageResize(AmazonS3 s3, AWSCredentials credentials, String objectKey, int pos, String suffix, BufferedImage src, int maxWidth) throws IOException {
		String thumbnailKey = objectKey.substring(0, pos - 1) + "_tumb" + maxWidth +"." + suffix;
		if (src != null && src.getWidth() > maxWidth) {
			BufferedImage bi = Scalr.resize(src, maxWidth);
			File tempFile = File.createTempFile("tempfile_",Long.toString(System.nanoTime()) + "." + suffix);
			ImageIO.write(bi, suffix, tempFile);
			s3.putObject(new PutObjectRequest(BUCKET_NAME, thumbnailKey, tempFile));
			tempFile.delete();
		} else {
			TransferManager tx = new TransferManager(credentials);
			tx.copy(BUCKET_NAME, objectKey, BUCKET_NAME, thumbnailKey);
		}
		
		System.out.println("File : " + thumbnailKey + ", Size : " + maxWidth);
	}
}