import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Created by gaurav_vishnoi on 9/24/15.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class S3ConnectionTest {

    private static final String bucketName = "s3-connector-test";
    private static final String key = "key-" + new Random().nextInt();
    private static AmazonS3 s3Connection;

    @BeforeClass
    public static void init() {
        String profile = "S3";
        S3Connection s3connectionClient = new S3Connection();
        s3Connection = s3connectionClient.connect(profile);
        s3Connection.createBucket(bucketName);
    }


    @Test
    public void test1CreateBucket() throws IOException {
        String bucketLocation = s3Connection.getBucketLocation(bucketName);
        Assert.assertNotNull(bucketLocation);

    }

    @Test
    public void test2StoreObject() throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("test.txt");
        ObjectMetadata metadata = new ObjectMetadata();
        Date date = Calendar.getInstance().getTime();
        metadata.setExpirationTime(date);

        s3Connection.putObject(bucketName, key, inputStream, metadata);
        S3Object s3Object = s3Connection.getObject(bucketName, key);
        Assert.assertNotNull(s3Object);
        S3ObjectInputStream objectContent = s3Object.getObjectContent();
        IOUtils.copy(objectContent, new FileOutputStream("src/test/resources/result.txt"));
    }

    @Test
    public void test3DeleteObject() throws IOException {
        s3Connection.deleteObject(bucketName, key);
        S3Object s3Object = null;
        try {
            s3Object = s3Connection.getObject(bucketName, key);
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getStatusCode() == 404);
        }
        Assert.assertNull(s3Object);
    }


    @Test
    public void test4UploadFile() {
        List<Bucket> buckets = s3Connection.listBuckets();
        System.out.print(buckets);
        Assert.assertNotNull(buckets);
    }


}