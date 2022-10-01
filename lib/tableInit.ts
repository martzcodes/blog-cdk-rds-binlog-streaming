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

const sm = new SecretsManagerClient({ region: "us-east-1" });

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
    dbname: database,
    host,
    username: user,
  } = JSON.parse(SecretString);

  try {
    const poolConfig: PoolConfig = {
      database: dbName || database,
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

        // Set the binlog retention
        await connection.query(
          "CALL mysql.rds_set_configuration('binlog retention hours', 24);"
        );

        // create a user that will be used for iam-based access
        await connection.query(
          "CREATE USER `binlog-streamer` IDENTIFIED WITH AWSAuthenticationPlugin as 'RDS'"
        );
        await connection.query(
          "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'ss-binlog'@'%'"
        );

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
