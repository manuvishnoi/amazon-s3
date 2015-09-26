import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by gaurav_vishnoi on 9/24/15.
 */
public class S3Connection {
    static Properties prop = new Properties();

    static {
        String propFileName = "s3Configuration.properties";
        InputStream inputStream = S3Connection.class.getClassLoader().getResourceAsStream(propFileName);
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public AmazonS3 connect(String profileName) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionMaxIdleMillis(new Long(prop.getProperty("connectionMaxIdleMillis")));
        clientConfiguration.setMaxConnections(new Integer(prop.getProperty("maxConnections")));
        clientConfiguration.setConnectionTimeout(new Integer(prop.getProperty("connectionTimeout")));
        clientConfiguration.setConnectionTTL(new Long(prop.getProperty("connectionTtl")));
        return new AmazonS3Client(new ProfileCredentialsProvider(profileName), clientConfiguration);
    }
}
