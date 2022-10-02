import {
  CloudFormationCustomResourceEvent,
  CloudFormationCustomResourceFailedResponse,
  CloudFormationCustomResourceSuccessResponse,
} from "aws-lambda";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { PoolConfig } from "mysql";
import { createPool, Pool } from "promise-mysql";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const sm = new SecretsManagerClient({ region: "us-east-1" });
const s3 = new S3Client({ region: "us-east-1" });

const sleep = async (timeout: number) =>
  new Promise((resolve) => setTimeout(resolve, timeout));

export const getConnectionPool = async (
  dbName?: string,
  retries = 0
): Promise<Pool> => {
  const { SecretString } = await sm.send(
    new GetSecretValueCommand({ SecretId: process.env.SECRET_ARN })
  );
  if (!SecretString) {
    throw new Error("Unable to fetch secret!");
  }
  const {
    password,
    dbname,
    host,
    username: user,
  } = JSON.parse(SecretString);

  try {
    const poolConfig: PoolConfig = {
      database: dbname,
      host,
      connectionLimit: 100,
      multipleStatements: true,
      password,
      user,
    };
    const pool = await createPool(poolConfig);
    return await checkConnection(pool, retries, dbName);
  } catch (e) {
    console.error(
      "An error occurred while creating a connection pool: ",
      (e as Error).message
    );
    throw e;
  }
};

const checkConnection = async (
  connection: Pool,
  retries: number,
  dbName?: string
): Promise<Pool> => {
  if (retries > 2) {
    throw new Error("Could not connect!");
  }
  try {
    await connection.query("select 1");
    return connection;
  } catch {
    console.log(`Couldn't connect on try #${++retries}`);
    await sleep(retries * 10000);
    return checkConnection(
      await getConnectionPool(dbName, retries),
      retries,
      dbName
    );
  }
};

export const handler = async (
  event: CloudFormationCustomResourceEvent
): Promise<
  | CloudFormationCustomResourceSuccessResponse
  | CloudFormationCustomResourceFailedResponse
> => {
  switch (event.RequestType) {
    case "Create":
      try {
        const connection = await getConnectionPool();
        // create a table
        await connection.query(`CREATE TABLE IF NOT EXISTS tasks (
            task_id INT AUTO_INCREMENT,
            title VARCHAR(255) NOT NULL,
            start_date DATE,
            due_date DATE,
            priority TINYINT NOT NULL DEFAULT 3,
            description TEXT,
            PRIMARY KEY (task_id)
        )`);

        // Set the binlog retention
        await connection.query(
          "CALL mysql.rds_set_configuration('binlog retention hours', 24);"
        );

        // Insert some rows into the table
        await connection.query(
          `INSERT INTO tasks(title,priority) VALUES('Create first task',1)`
        );
        await connection.query(
          `INSERT INTO tasks(title,priority) VALUES('Create another first task',1)`
        );

        // Delete a task
        await connection.query(`DELETE FROM tasks WHERE title = 'Create another first task'`)

        // Insert multiple rows in a single query
        await connection.query(`INSERT INTO tasks(title, priority) VALUES ('Task 1/3', 1), ('Task 2/2',2), ('Task 3/3',3)`);

        // Update a task (fix a typo)
        await connection.query(`UPDATE tasks SET title = 'Task 2/3' WHERE title = 'Task 2/2'`);

        // get the server id and save it to s3
        const serverId = await connection.query(`SELECT @@server_id`);
        const command = new PutObjectCommand({
          Key: 'serverId.json',
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
    default:
      console.error("No op for", event.RequestType);
      return { ...event, PhysicalResourceId: "retention", Status: "SUCCESS" };
  }
};
