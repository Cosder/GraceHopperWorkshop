
package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App implements RequestHandler<DynamodbEvent, String> {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    @Override
    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        log.info("Received DynamoDB event: {}", ddbEvent);

        try {
            if (ddbEvent.getRecords().size() > 1) {
                log.error("More than 1 record received in the batch. Event: {}", ddbEvent);
                throw new IllegalArgumentException("Batch size greater than 1 is not supported.");
            }

            DynamodbEvent.DynamodbStreamRecord record = ddbEvent.getRecords().get(0);
            if (record.getDynamodb() != null && record.getDynamodb().getNewImage() != null) {
                log.info("Processing record: {}", record.getDynamodb().getNewImage());
                log.info("Record ID: {}, Event Name: {}", record.getEventID(), record.getEventName());
                log.info("Record details: {}", record);
            }

        } catch (Exception e) {
            log.error("Error processing DynamoDB event", e);
            throw e;
        }

        return "Processing complete";
    }
}