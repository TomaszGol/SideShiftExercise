import { strict as assert } from "assert";
import * as ethers from "ethers";
import pMap from "p-map";
import { z } from "zod";
import axios from "axios";
// `ns` is our own wrapper for bignumber.js to deal with mathematic operations for strings
import { ns } from "@sideshift/shared";
// `Order` is a TypeORM Entity, i.e a database table
// `memGetInternalGqlc` returns a GraphQL client memoized by lodash.memoize function
// `RedisTaskQueue` is a messaging queue that utilizes Redis to store messages
import {
  Order,
  createLogger,
  memGetInternalGqlc,
  RedisTaskQueue,
} from "@sideshift/shared-node";
// returns a unique ID to identify deposits
import { getEthereumNativeDepositUniqueId } from "./shared";
// `context` stores application data, it uses `p-lazy` for efficiency
import { contextLazy } from "./context";

// HACK: Can't find this exported from ethers
type BlocksWithTransactions = Awaited<
  ReturnType<ethers.providers.BaseProvider["getBlockWithTransactions"]>
>;

const accountTxListResultSchema = z.array(
  z.object({
    hash: z.string(),
    from: z.string(),
    to: z.string(),
    value: z.string(),
  })
);

/**
 * Checks specific deposit addresses for missed deposits
 */
export const runConfirmedNativeTokenExtraWorker = async (): Promise<void> => {
  const {
    db,
    nativeMethod,
    network,
    nodeProvider,
    config: { etherscanApiKey, evmAccount: account },
  } = await contextLazy;

  const { asset, id: depositMethodId } = nativeMethod;

  const logger = createLogger("ethereum:deposit:confirmed-native");
  const graphQLClient = memGetInternalGqlc();

  async function fetchOrderForAddress(
    address: string
  ): Promise<Order | undefined> {
    const order = await db
      .getRepository(Order)
      .createQueryBuilder("o")
      .select()
      .where(`deposit_method = :depositMethodId`, { depositMethodId })
      .andWhere(`deposit_address->>'address' = :address`, { address })
      .getOne();

    return order ?? undefined;
  }

  const scanTxid = async (tx: BlocksWithTransactions["transactions"][0]) => {
    assert.equal(typeof tx, "object", "tx is not object");
    assert(tx.from, "from missing");

    if (typeof tx.hash !== "string") {
      throw new Error(`txid must be string`);
    }

    if (typeof tx.blockHash !== "string") {
      throw new Error(`blockHash must be string`);
    }

    const isGasPriceAndToAddressCorrect =
      tx.to &&
      tx.to.toLowerCase() === account.toLowerCase() &&
      tx.gasPrice !== undefined;

    if (!isGasPriceAndToAddressCorrect) {
      return false;
    }

    const total = ns.sum(
      tx.value.toString(),
      ns.times(tx.gasLimit.toString(), tx.gasPrice.toString())
    );

    const order = await fetchOrderForAddress(tx.from);

    if (!order) {
      return false;
    }

    const valueAsEther = ethers.utils.formatEther(tx.value);
    const totalAsEther = ethers.utils.formatEther(total);

    const wasCredited = await graphQLClient.maybeInternalCreateDeposit({
      orderId: order.id,
      tx: {
        txid: tx.hash,
      },
      amount: totalAsEther,
      uniqueId: getEthereumNativeDepositUniqueId(nativeMethod, tx.hash),
    });

    if (!wasCredited) {
      return false;
    }

    logger.info(
      `Stored deposit. ${tx.hash}. ${valueAsEther} ${asset} for order ${order.id}`
    );

    return true;
  };

  const getEtherScanTxListForAddress = async (
    address: string,
    blockNumber: string
  ) => {
    const txList = await axios
      .get(
        `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=${blockNumber}&sort=desc&apikey=${etherscanApiKey}`
      )
      .then((res) => accountTxListResultSchema.parse(res));

    return txList;
  };

  const getEtherScanBlocknumber = async (timestamp: string) => {
    try {
      const blockNumber = await axios.get(
        `https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=${timestamp}&closest=before&apikey=${etherscanApiKey}`
      );
      return blockNumber.result;
    } catch (error: any) {
      logger.error(
        error,
        "Error fetching block number for order timestamp %s: %s",
        timestamp,
        error.message
      );

      return;
    }
  };

  const runScanByOrderId = async () => {
    const queue = await RedisTaskQueue.queues.evmNativeConfirm(network, true);

    if (!etherscanApiKey) {
      logger.error("Etherscan not configured");

      return;
    }

    await queue.run(async (orderId) => {
      logger.info(
        "Processing queued task to look at order %s for deposits",
        orderId
      );

      const order = await db.getRepository(Order).findOneBy({ id: orderId });

      if (!order) {
        logger.error("Order %s not found", orderId);

        return;
      }

      if (!order.depositAddress) {
        // The deposit address may have been unassigned
        logger.error("Order %s has no deposit address", orderId);

        return;
      }

      const blockNumber = await getEtherScanBlocknumber(
        order.createdAt.getTime()
      );

      if (!blockNumber) {
        return;
      }

      let txs;

      try {
        // In descending order
        txs = await getEtherScanTxListForAddress(
          order.depositAddress.address,
          blockNumber
        );
      } catch (error: any) {
        logger.error(
          error,
          "Error fetching txs for order %s: %s",
          orderId,
          error.message
        );

        return;
      }

      // Only transactions with a value and only the first 10 transactions
      // Could use pagination instead of slice, but then we can get less than 10 transactions if value was '0'.
      const trimmedTxs = txs
        .filter((tx) => ethers.BigNumber.from(tx.value).gt(0))
        .slice(0, 10);

      logger.info("Found %s transactions for order %s", txs.length, orderId);

      await pMap(trimmedTxs, async (etherscanTx) => {
        const ethersTx = await nodeProvider.getTransaction(etherscanTx.hash);

        if (!ethersTx) {
          logger.error("Transaction %s not found", etherscanTx.hash);

          return;
        }

        logger.info("Scanning tx %s", etherscanTx.hash);

        await scanTxid(ethersTx);
      });
    });
  };

  await runScanByOrderId();
};
