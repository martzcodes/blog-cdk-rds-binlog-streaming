import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { PoolConfig } from "mysql";
import { createPool, Pool } from "promise-mysql";

const sm = new SecretsManagerClient({ region: "us-east-1" });

let password: string, dbname: string, host: string, user: string;

const sleep = async (timeout: number) =>
  new Promise((resolve) => setTimeout(resolve, timeout));

export const getConnectionPool = async (
  retries = 0
): Promise<Pool> => {
  if (!password) {
    const { SecretString } = await sm.send(
      new GetSecretValueCommand({ SecretId: process.env.SECRET_ARN })
    );
    if (!SecretString) {
      throw new Error("Unable to fetch secret!");
    }
    const secret = JSON.parse(SecretString);
    password = secret.password;
    dbname = secret.dbname;
    user = secret.username;
    host = secret.host;
  }

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
    return await checkConnection(pool, retries);
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
      await getConnectionPool(retries),
      retries,
    );
  }
};
