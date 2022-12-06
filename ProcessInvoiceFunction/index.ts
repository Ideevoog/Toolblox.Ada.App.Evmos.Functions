import { AzureFunction, Context } from "@azure/functions";
import { Edm, TableClient } from "@azure/data-tables";
import * as redstone from 'redstone-api';
import * as ethers from 'ethers';
const nearAPI = require("near-api-js");
const { keyStores, KeyPair, connect  } = nearAPI;
const myKeyStore = new keyStores.InMemoryKeyStore();
const PRIVATE_KEY = process.env.NearAccountPrivateKey;
const EVM_PRIVATE_KEY = process.env.EvmAccountPrivateKey;
// creates a public / private key pair using the provided private key
const keyPair = KeyPair.fromString(PRIVATE_KEY);
const connectionConfig = {
    networkId: "testnet",
    keyStore: myKeyStore, // first create a key store 
    nodeUrl: "https://rpc.testnet.near.org",
    walletUrl: "https://wallet.testnet.near.org",
    helperUrl: "https://helper.testnet.near.org",
    explorerUrl: "https://explorer.testnet.near.org",
  };

const tableStorageConnection = process.env["adawillhandlestorage_STORAGE"] || "";

const queueTrigger: AzureFunction = async function (context: Context, myQueueItem: string): Promise<void> {
  const client = TableClient.fromConnectionString(tableStorageConnection, `Invoices`);
  let accountantId = myQueueItem.split(':')[0].replace(/\"/gi, "");
  const invoice = await client.getEntity<Invoice>(accountantId, myQueueItem.split(':')[1]);

  // adds the keyPair you created to keyStore 
  await myKeyStore.setKey("testnet", "accountant-ada.testnet", keyPair);
  try {
    const accountantClient = TableClient.fromConnectionString(tableStorageConnection, `Accountants`);
    const accountantList = accountantClient.listEntities<Accountant>({ 
      queryOptions: { 
        filter: `Contract eq '${accountantId}' or Workflow eq '${accountantId}'`, 
      }, 
    });
    let accountant : Accountant = undefined;
    for await (const accountantLine of accountantList) {
      accountant = accountantLine;
      break;
    }
    if (accountant == undefined)
    {
      throw new Error('Cannot find accountant with id ' + accountantId);
    }

    let processFee: number = Number(accountant.ProcessFee);

    var alternativeCurrency = invoice.AlternativeCurrency;
    var alternativeFxValue = invoice.AlternativeFxValue;

    //todo get alternative currency
    if (!invoice.IsFiat && invoice.AlternativeFxValue == undefined)
    {
      if (invoice.Currency == "NEAR")
      {
        //get multiplier for wrap.testnet
        const nearConnection = await connect(connectionConfig);
        const oracleContract = new nearAPI.Contract(await nearConnection.account("accountant-ada.testnet"), "priceoracle.testnet", {
          viewMethods: ['get_price_data'],
          changeMethods: []
        });
        var multiplier = (await oracleContract.get_price_data({ "asset_ids": ["wrap.testnet"] })).prices[0].price.multiplier;
        alternativeFxValue = (Number(multiplier.toString()) / 10000).toString();
        alternativeCurrency = "USD";
      }
      else{
        try{
          const price = await redstone.getPrice(invoice.Currency.toUpperCase());
          alternativeFxValue = price.value.toString();
          alternativeCurrency = "USD";
        }
        catch(ex)
        {
          console.error(ex);
        }
      }
    }
    
    if (invoice.ProcessedAt == undefined)
    {
      if (accountant.SelectedChain == 1)
      {
        const nearConnection = await connect(connectionConfig);
        const contract = new nearAPI.Contract(await nearConnection.account("accountant-ada.testnet"), accountant.Workflow, {
            viewMethods: ['getItem'],
            changeMethods: ['process', 'processExternal']
          });
        //Near
        if (invoice.InvoiceNr == undefined || Number(invoice.InvoiceNr) == 0)
        {
          console.log("Running processExternal");
          var item = await contract.processExternal({ "name" : invoice.Article, "amount" : invoice.Amount, "currency" : invoice.Currency, "from" : invoice.From, "to" : invoice.To, "receipt" : invoice.rowKey, "processFee" : processFee.toString() });
          var itemId = item.id;
          invoice.InvoiceNr = BigInt(itemId);
        }else{
          //process
          console.log("Running process for invoice " + invoice.InvoiceNr);
          await contract.process({ "id" : Number(invoice.InvoiceNr), "receipt": invoice.rowKey, "processFee" : processFee.toString() });
        }
      }
      if (accountant.SelectedChain == 5)
      {
        //evmos
        const signer = new ethers.Wallet(new ethers.utils.SigningKey(EVM_PRIVATE_KEY), new ethers.providers.JsonRpcProvider("https://eth.bd.evmos.dev:8545"));
        const abi = JSON.parse('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"string","name":"article","type":"string"},{"indexed":false,"internalType":"string","name":"currency","type":"string"},{"indexed":false,"internalType":"string","name":"amount","type":"string"}],"name":"IssueInvoice","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_id","type":"uint256"}],"name":"ItemUpdated","type":"event"},{"stateMutability":"payable","type":"fallback"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"accountantList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"adr","type":"address"}],"name":"addAccountant","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"adr","type":"address"}],"name":"addSource","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"fromList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"}],"name":"generate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getItem","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"cnt","type":"uint256"}],"name":"getLatest","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getName","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"cursor","type":"uint256"},{"internalType":"uint256","name":"howMany","type":"uint256"},{"internalType":"bool","name":"onlyMine","type":"bool"}],"name":"getPage","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getStatus","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"items","outputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"uint256","name":"processFee","type":"uint256"}],"name":"process","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"uint256","name":"processFee","type":"uint256"}],"name":"processExternal","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"sourceList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"toList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]');
        const contract = new ethers.Contract(accountant.Workflow, abi, signer);
        if (invoice.InvoiceNr == undefined || Number(invoice.InvoiceNr) == 0)
        {
          let itemTransaction = await contract.processExternal(invoice.Article, invoice.Amount, invoice.Currency, invoice.From, invoice.To, invoice.rowKey, BigInt(processFee / 1000), { "gasLimit" : 3000000 });
          let receipt = await itemTransaction.wait();
          invoice.InvoiceNr = BigInt(receipt.events.at(-1).args["_id"]);
        }
        else
        {
          let itemTransaction = await contract.process(BigInt(Number(invoice.InvoiceNr)), invoice.rowKey, BigInt(processFee / 1000), { "gasLimit" : 3000000 });
          await itemTransaction.wait();
        }
      }
    }

    invoice.ProcessedAt = new Date();
    invoice.ProcessFee = accountant.ProcessFee;
    invoice.Error = '';
    invoice.AlternativeCurrency = alternativeCurrency;
    invoice.AlternativeFxValue = alternativeFxValue;
    await client.upsertEntity(invoice, "Merge");
    context.bindings.outQueueItem = myQueueItem;
  } catch (error) {
    const errorEntity = {
      partitionKey: invoice.partitionKey,
      rowKey: invoice.rowKey,
      Error: error.toString(),
    };
    await client.upsertEntity(errorEntity, "Merge");
    throw error;
  }
};

interface Invoice {
  partitionKey: string;
  rowKey: string;
  InvoiceNr? : bigint;
  CreatedAt: Date;
  From? : string;
  To? : string;
  Article? : string;
  ProcessedAt?: Date;
  Amount? : string;
  IsFiat?: boolean;
  ProcessFee?: number;
  Error? : string;
  Currency? : string;
  AlternativeCurrency? : string;
  AlternativeFxValue? : string;
}
interface Accountant {
  partitionKey: string;
  rowKey: string;
  ProcessFee: number;
  Workflow: string;
  SelectedChain: number;
}
export default queueTrigger;
