import {
  CloudFormationCustomResourceEvent,
  CloudFormationCustomResourceFailedResponse,
  CloudFormationCustomResourceSuccessResponse,
} from "aws-lambda";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getConnectionPool } from "./db";

const s3 = new S3Client({ region: "us-east-1" });

export const handler = async (
  event: CloudFormationCustomResourceEvent
): Promise<
  | CloudFormationCustomResourceSuccessResponse
  | CloudFormationCustomResourceFailedResponse
> => {
  if (event.RequestType === "Create") {
    try {
      const connection = await getConnectionPool();

      // Set the binlog retention
      await connection.query(
        "CALL mysql.rds_set_configuration('binlog retention hours', 24);"
      );

      // create a table
      await connection.query(`CREATE TABLE IF NOT EXISTS tasks (
        task_id INT AUTO_INCREMENT,
        title VARCHAR(255) NOT NULL,
        priority TINYINT NOT NULL DEFAULT 3,
        PRIMARY KEY (task_id)
    )`);

      // Insert some rows into the table
      await connection.query(
        `INSERT INTO tasks(title,priority) VALUES('Create first task',1)`
      );
      await connection.query(
        `INSERT INTO tasks(title,priority) VALUES('Create another first task',1)`
      );

      // Delete a task
      await connection.query(
        `DELETE FROM tasks WHERE title = 'Create another first task'`
      );

      // Insert multiple rows in a single query
      await connection.query(
        `INSERT INTO tasks(title, priority) VALUES ('Task 1/3', 1), ('Task 2/2',2), ('Task 3/3',3)`
      );

      // Update a task (fix a typo)
      await connection.query(
        `UPDATE tasks SET title = 'Task 2/3' WHERE title = 'Task 2/2'`
      );

      // get the server id and save it to s3
      const serverId = await connection.query(`SELECT @@server_id`);
      const command = new PutObjectCommand({
        Key: "serverId.json",
        Bucket: process.env.BUCKET_NAME,
        Body: JSON.stringify(serverId[0]),
      });
      await s3.send(command);

      return { ...event, PhysicalResourceId: "retention", Status: "SUCCESS" };
    } catch (e) {
      console.error(`retention initialization failed!`, e);
      return {
        ...event,
        PhysicalResourceId: "retention",
        Reason: (e as Error).message,
        Status: "FAILED",
      };
    }
  }
  return { ...event, PhysicalResourceId: "retention", Status: "SUCCESS" };
};
