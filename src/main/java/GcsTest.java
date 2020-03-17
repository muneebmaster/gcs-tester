import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.concurrent.Callable;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.FileUtils;

// Imports the Google Cloud client library
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.Lists;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "GcsTest", mixinStandardHelpOptions = true, version = "GcsTest 1.0",
        description = "Connects to a Google Cloud Storage bucket and prints items.")
class GcsTest implements Callable<Integer> {
    //@Parameters(index = "0", description = "The file whose checksum to calculate.")
    //private File file;

    @Option(names = { "-j", "--json" }, required = true, paramLabel = "<json file>", description = "The service account's key as JSON")
    private File json;

    @Option(names = { "-b", "--bucket" }, required = true, paramLabel = "<GCS bucket>", description = "The Google Cloud Storage bucket name")
    private String bucketName;

    public static void main(String... args) {
        /*
        if (args.length < 2) {
            System.err.println("Missing required CLI params: <json key file> <GCS bucket name>");
            return;
        }

        File jsonKeyFile = new File(args[0]);
        if (! jsonKeyFile.exists()) {
            System.err.println("JSON key file does not exist!");
            return;
        }

        String bucketName = args[1];
        if (bucketName.length() == 0) {
            System.err.println("Bucket name is missing!");
            return;
        }*/

        new CommandLine(new GcsTest()).execute(args);
        //System.exit(exitCode);
    }


    @Override
    public Integer call() throws Exception {
        if (! json.exists()) {
            System.err.println("JSON key file does not exist!");
            return 1;
        }

        String proxy;
        if ((proxy = System.getProperty("https.proxyHost")) != null) {
            proxy += ":" + System.getProperty("https.proxyPort");
            System.out.println("* DEBUG * Connecting through https proxy " + proxy);
        }

        GoogleCredentials credentials =
                GoogleCredentials
                        .fromStream(
                                new ByteArrayInputStream(FileUtils.readFileToByteArray(json)))
                        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

        System.out.println("* DEBUG * Setting credentials...");
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        System.out.println("* DEBUG * Connecting to bucket " + bucketName);
        Bucket bucket = storage.get(bucketName);
        System.out.println("* DEBUG * List of items in the bucket:");
        Page<Blob> buckets = bucket.list();
        for (Blob blob : buckets.iterateAll()) {
            System.out.println(blob.toString());
        }

        return 0;
    }
}
