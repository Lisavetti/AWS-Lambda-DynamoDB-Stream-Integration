const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const { marshall } = require("@aws-sdk/util-dynamodb");
const { v4: uuidv4 } = require("uuid");

const dynamoDB = new DynamoDBClient({ region: process.env.region });

// Ensure correct table reference
const AUDIT_TABLE = process.env.target_table;

module.exports.handler = async (event, context) => {
    const logger = context.logger || console;

    try {
        const putRequests = event.Records.map(async (record) => {
            const eventName = record.eventName;
            const timestamp = new Date().toISOString();
            const itemKey = record.dynamodb.Keys.key.S;
            const auditId = uuidv4();

            const baseItem = {
                id: auditId,
                itemKey: itemKey,
                modificationTime: timestamp
            };

            if (eventName === "INSERT") {
                const newImage = record.dynamodb.NewImage;
                baseItem.newValue = {
                    key: newImage.key.S,
                    value: parseInt(newImage.value.N, 10)
                };
            } else if (eventName === "MODIFY") {
                const newImage = record.dynamodb.NewImage;
                const oldImage = record.dynamodb.OldImage;
                baseItem.oldValue = parseInt(oldImage.value.N, 10);
                baseItem.newValue = parseInt(newImage.value.N, 10);
            } else {
                return;  // Ignore REMOVE or other events
            }

            logger.log("Saving audit record:", JSON.stringify(baseItem));

            const command = new PutItemCommand({
                TableName: AUDIT_TABLE,  // Correct target table
                Item: marshall(baseItem),
            });

            await dynamoDB.send(command);
        });

        await Promise.all(putRequests);

        return { statusCode: 200, body: JSON.stringify({ message: "Audit records saved." }) };
    } catch (error) {
        logger.error("Error processing stream: ", error);
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};
