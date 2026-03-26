const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");

const DATA_ROOT = path.resolve(__dirname, "../data");
let cachedGraphPromise = null;

function nodeId(type, rawId) {
  return `${type}:${rawId}`;
}

function edgeKey(type, from, to) {
  return `${type}:${from}->${to}`;
}

function firstValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return String(value);
    }
  }

  return "";
}

function addNode(nodes, nodeIndex, type, rawId, data, label) {
  if (!rawId) {
    return null;
  }

  const id = nodeId(type, rawId);

  if (nodeIndex.has(id)) {
    return nodeIndex.get(id);
  }

  const node = {
    id,
    type,
    label: label || String(rawId),
    data,
  };

  nodeIndex.set(id, node);
  nodes.push(node);
  return node;
}

function addEdge(edges, edgeIndex, type, from, to, data = {}) {
  if (!from || !to) {
    return;
  }

  const id = edgeKey(type, from, to);

  if (edgeIndex.has(id)) {
    return;
  }

  edgeIndex.add(id);
  edges.push({
    id,
    from,
    to,
    type,
    ...data,
  });
}

async function loadCsvFile(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

async function loadJsonLinesFile(filePath) {
  const content = await fs.promises.readFile(filePath, "utf8");
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function loadRowsFromFile(filePath) {
  if (filePath.endsWith(".csv")) {
    return loadCsvFile(filePath);
  }

  if (filePath.endsWith(".jsonl") || filePath.endsWith(".json")) {
    return loadJsonLinesFile(filePath);
  }

  return [];
}

async function loadRowsFromDirectory(directoryName) {
  const directoryPath = path.join(DATA_ROOT, directoryName);
  const entries = await fs.promises.readdir(directoryPath, {
    withFileTypes: true,
  });
  const files = entries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter(
      (name) =>
        name.endsWith(".jsonl") ||
        name.endsWith(".json") ||
        name.endsWith(".csv"),
    )
    .sort();

  const rows = [];

  for (const fileName of files) {
    const fileRows = await loadRowsFromFile(path.join(directoryPath, fileName));
    rows.push(...fileRows);
  }

  return rows;
}

async function buildGraph() {
  if (!cachedGraphPromise) {
    cachedGraphPromise = (async () => {
      const [
        salesOrders,
        salesOrderItems,
        salesOrderScheduleLines,
        deliveryHeaders,
        deliveryItems,
        billingHeaders,
        billingItems,
        billingCancellations,
        journalEntries,
        payments,
        businessPartners,
        customerSalesAreas,
        customerCompanies,
        products,
        productDescriptions,
        productPlants,
        productStorageLocations,
        plants,
      ] = await Promise.all([
        loadRowsFromDirectory("sales_order_headers"),
        loadRowsFromDirectory("sales_order_items"),
        loadRowsFromDirectory("sales_order_schedule_lines"),
        loadRowsFromDirectory("outbound_delivery_headers"),
        loadRowsFromDirectory("outbound_delivery_items"),
        loadRowsFromDirectory("billing_document_headers"),
        loadRowsFromDirectory("billing_document_items"),
        loadRowsFromDirectory("billing_document_cancellations"),
        loadRowsFromDirectory("journal_entry_items_accounts_receivable"),
        loadRowsFromDirectory("payments_accounts_receivable"),
        loadRowsFromDirectory("business_partners"),
        loadRowsFromDirectory("customer_sales_area_assignments"),
        loadRowsFromDirectory("customer_company_assignments"),
        loadRowsFromDirectory("products"),
        loadRowsFromDirectory("product_descriptions"),
        loadRowsFromDirectory("product_plants"),
        loadRowsFromDirectory("product_storage_locations"),
        loadRowsFromDirectory("plants"),
      ]);

      const nodes = [];
      const edges = [];
      const nodeIndex = new Map();
      const edgeIndex = new Set();

      const salesOrderNodeByOrder = new Map();
      const salesOrderItemNodeByKey = new Map();
      const scheduleLineNodeByKey = new Map();
      const deliveryNodeByDocument = new Map();
      const deliveryItemNodeByKey = new Map();
      const billingNodeByDocument = new Map();
      const billingItemNodeByKey = new Map();
      const journalEntryNodeByKey = new Map();
      const paymentNodeByKey = new Map();
      const businessPartnerNodeById = new Map();
      const customerSalesAreaNodeByKey = new Map();
      const customerCompanyNodeByKey = new Map();
      const productNodeById = new Map();
      const productDescriptionNodeByKey = new Map();
      const productPlantNodeByKey = new Map();
      const productStorageLocationNodeByKey = new Map();
      const plantNodeById = new Map();

      salesOrders.forEach((row) => {
        const orderId = firstValue(row.salesOrder, row.SalesOrder);
        const node = addNode(
          nodes,
          nodeIndex,
          "sales_order",
          orderId,
          row,
          orderId,
        );
        if (node) {
          salesOrderNodeByOrder.set(orderId, node);
        }
      });

      salesOrderItems.forEach((row) => {
        const orderId = firstValue(row.salesOrder, row.SalesOrder);
        const itemId = firstValue(row.salesOrderItem, row.SalesOrderItem);
        const node = addNode(
          nodes,
          nodeIndex,
          "sales_order_item",
          `${orderId}-${itemId}`,
          row,
          `${orderId}-${itemId}`,
        );
        if (node) {
          salesOrderItemNodeByKey.set(`${orderId}|${itemId}`, node);
          addEdge(
            edges,
            edgeIndex,
            "order_contains_item",
            nodeId("sales_order", orderId),
            node.id,
          );
        }
      });

      salesOrderScheduleLines.forEach((row) => {
        const orderId = firstValue(row.salesOrder, row.SalesOrder);
        const itemId = firstValue(row.salesOrderItem, row.SalesOrderItem);
        const scheduleLine = firstValue(row.scheduleLine, row.ScheduleLine);
        const node = addNode(
          nodes,
          nodeIndex,
          "sales_order_schedule_line",
          `${orderId}-${itemId}-${scheduleLine}`,
          row,
          `${orderId}-${itemId}-${scheduleLine}`,
        );
        if (node) {
          scheduleLineNodeByKey.set(
            `${orderId}|${itemId}|${scheduleLine}`,
            node,
          );
          addEdge(
            edges,
            edgeIndex,
            "item_contains_schedule_line",
            nodeId("sales_order_item", `${orderId}-${itemId}`),
            node.id,
          );
        }
      });

      deliveryHeaders.forEach((row) => {
        const deliveryDocument = firstValue(
          row.deliveryDocument,
          row.DeliveryDocument,
        );
        const node = addNode(
          nodes,
          nodeIndex,
          "delivery",
          deliveryDocument,
          row,
          deliveryDocument,
        );
        if (node) {
          deliveryNodeByDocument.set(deliveryDocument, node);
        }
      });

      deliveryItems.forEach((row) => {
        const deliveryDocument = firstValue(
          row.deliveryDocument,
          row.DeliveryDocument,
        );
        const deliveryItem = firstValue(
          row.deliveryDocumentItem,
          row.DeliveryDocumentItem,
        );
        const node = addNode(
          nodes,
          nodeIndex,
          "delivery_item",
          `${deliveryDocument}-${deliveryItem}`,
          row,
          `${deliveryDocument}-${deliveryItem}`,
        );
        if (node) {
          deliveryItemNodeByKey.set(
            `${deliveryDocument}|${deliveryItem}`,
            node,
          );
          addEdge(
            edges,
            edgeIndex,
            "delivery_contains_item",
            nodeId("delivery", deliveryDocument),
            node.id,
          );

          const sourceOrder = firstValue(
            row.referenceSdDocument,
            row.ReferenceSdDocument,
          );
          const sourceItem = firstValue(
            row.referenceSdDocumentItem,
            row.ReferenceSdDocumentItem,
          );
          addEdge(
            edges,
            edgeIndex,
            "item_fulfills_order_item",
            nodeId("sales_order_item", `${sourceOrder}-${sourceItem}`),
            node.id,
          );
        }
      });

      billingHeaders.forEach((row) => {
        const billingDocument = firstValue(
          row.billingDocument,
          row.BillingDocument,
        );
        const node = addNode(
          nodes,
          nodeIndex,
          "billing",
          billingDocument,
          row,
          billingDocument,
        );
        if (node) {
          billingNodeByDocument.set(billingDocument, node);

          const customerId = firstValue(row.soldToParty, row.SoldToParty);
          addEdge(
            edges,
            edgeIndex,
            "billing_belongs_to_customer",
            node.id,
            nodeId("business_partner", customerId),
          );
        }
      });

      billingItems.forEach((row) => {
        const billingDocument = firstValue(
          row.billingDocument,
          row.BillingDocument,
        );
        const billingItem = firstValue(
          row.billingDocumentItem,
          row.BillingDocumentItem,
        );
        const node = addNode(
          nodes,
          nodeIndex,
          "billing_item",
          `${billingDocument}-${billingItem}`,
          row,
          `${billingDocument}-${billingItem}`,
        );
        if (node) {
          billingItemNodeByKey.set(`${billingDocument}|${billingItem}`, node);
          addEdge(
            edges,
            edgeIndex,
            "billing_contains_item",
            nodeId("billing", billingDocument),
            node.id,
          );

          const sourceDocument = firstValue(
            row.referenceSdDocument,
            row.ReferenceSdDocument,
          );
          const sourceItem = firstValue(
            row.referenceSdDocumentItem,
            row.ReferenceSdDocumentItem,
          );
          addEdge(
            edges,
            edgeIndex,
            "billing_item_from_delivery_item",
            nodeId("delivery_item", `${sourceDocument}-${sourceItem}`),
            node.id,
          );
        }
      });

      billingCancellations.forEach((row) => {
        const billingDocument = firstValue(
          row.billingDocument,
          row.BillingDocument,
        );
        const node = addNode(
          nodes,
          nodeIndex,
          "billing_cancellation",
          billingDocument,
          row,
          billingDocument,
        );
        if (node) {
          addEdge(
            edges,
            edgeIndex,
            "billing_cancelled_by",
            node.id,
            nodeId("billing", billingDocument),
          );
        }
      });

      journalEntries.forEach((row) => {
        const accountingDocument = firstValue(
          row.accountingDocument,
          row.AccountingDocument,
        );
        const accountingDocumentItem = firstValue(
          row.accountingDocumentItem,
          row.AccountingDocumentItem,
        );
        const key = `${accountingDocument}-${accountingDocumentItem}`;
        const node = addNode(
          nodes,
          nodeIndex,
          "journal_entry_item",
          key,
          row,
          key,
        );
        if (node) {
          journalEntryNodeByKey.set(
            `${accountingDocument}|${accountingDocumentItem}`,
            node,
          );

          const billingDocument = firstValue(
            row.referenceDocument,
            row.ReferenceDocument,
          );
          if (billingDocument) {
            addEdge(
              edges,
              edgeIndex,
              "journal_references_billing",
              node.id,
              nodeId("billing", billingDocument),
            );
          }

          const customerId = firstValue(row.customer, row.Customer);
          if (customerId) {
            addEdge(
              edges,
              edgeIndex,
              "journal_belongs_to_customer",
              node.id,
              nodeId("business_partner", customerId),
            );
          }
        }
      });

      payments.forEach((row) => {
        const accountingDocument = firstValue(
          row.accountingDocument,
          row.AccountingDocument,
        );
        const accountingDocumentItem = firstValue(
          row.accountingDocumentItem,
          row.AccountingDocumentItem,
        );
        const key = `${accountingDocument}-${accountingDocumentItem}`;
        const node = addNode(nodes, nodeIndex, "payment", key, row, key);
        if (node) {
          paymentNodeByKey.set(
            `${accountingDocument}|${accountingDocumentItem}`,
            node,
          );

          const journalNode = journalEntryNodeByKey.get(
            `${accountingDocument}|${accountingDocumentItem}`,
          );
          if (journalNode) {
            addEdge(
              edges,
              edgeIndex,
              "journal_to_payment",
              journalNode.id,
              node.id,
            );
          }

          const customerId = firstValue(row.customer, row.Customer);
          if (customerId) {
            addEdge(
              edges,
              edgeIndex,
              "payment_belongs_to_customer",
              node.id,
              nodeId("business_partner", customerId),
            );
          }
        }
      });

      businessPartners.forEach((row) => {
        const businessPartner = firstValue(
          row.businessPartner,
          row.BusinessPartner,
          row.customer,
          row.Customer,
        );
        const node = addNode(
          nodes,
          nodeIndex,
          "business_partner",
          businessPartner,
          row,
          firstValue(
            row.businessPartnerName,
            row.businessPartnerFullName,
            businessPartner,
          ),
        );
        if (node) {
          businessPartnerNodeById.set(businessPartner, node);
        }
      });

      customerSalesAreas.forEach((row) => {
        const customer = firstValue(row.customer, row.Customer);
        const salesOrganization = firstValue(
          row.salesOrganization,
          row.SalesOrganization,
        );
        const distributionChannel = firstValue(
          row.distributionChannel,
          row.DistributionChannel,
        );
        const division = firstValue(row.division, row.Division);
        const key = `${customer}-${salesOrganization}-${distributionChannel}-${division}`;
        const node = addNode(
          nodes,
          nodeIndex,
          "customer_sales_area",
          key,
          row,
          key,
        );
        if (node) {
          customerSalesAreaNodeByKey.set(key, node);
          addEdge(
            edges,
            edgeIndex,
            "customer_has_sales_area",
            nodeId("business_partner", customer),
            node.id,
          );
        }
      });

      customerCompanies.forEach((row) => {
        const customer = firstValue(row.customer, row.Customer);
        const companyCode = firstValue(row.companyCode, row.CompanyCode);
        const key = `${customer}-${companyCode}`;
        const node = addNode(
          nodes,
          nodeIndex,
          "customer_company",
          key,
          row,
          key,
        );
        if (node) {
          customerCompanyNodeByKey.set(key, node);
          addEdge(
            edges,
            edgeIndex,
            "customer_has_company_assignment",
            nodeId("business_partner", customer),
            node.id,
          );
        }
      });

      products.forEach((row) => {
        const product = firstValue(row.product, row.Product);
        const node = addNode(
          nodes,
          nodeIndex,
          "product",
          product,
          row,
          product,
        );
        if (node) {
          productNodeById.set(product, node);
        }
      });

      productDescriptions.forEach((row) => {
        const product = firstValue(row.product, row.Product);
        const language = firstValue(row.language, row.Language);
        const key = `${product}-${language}`;
        const node = addNode(
          nodes,
          nodeIndex,
          "product_description",
          key,
          row,
          firstValue(row.productDescription, key),
        );
        if (node) {
          productDescriptionNodeByKey.set(key, node);
          addEdge(
            edges,
            edgeIndex,
            "product_has_description",
            nodeId("product", product),
            node.id,
          );
        }
      });

      productPlants.forEach((row) => {
        const product = firstValue(row.product, row.Product);
        const plant = firstValue(row.plant, row.Plant);
        const key = `${product}-${plant}`;
        const node = addNode(nodes, nodeIndex, "product_plant", key, row, key);
        if (node) {
          productPlantNodeByKey.set(key, node);
          addEdge(
            edges,
            edgeIndex,
            "product_assigned_to_plant",
            nodeId("product", product),
            node.id,
          );
          addEdge(
            edges,
            edgeIndex,
            "product_plant_to_plant",
            node.id,
            nodeId("plant", plant),
          );
        }
      });

      productStorageLocations.forEach((row) => {
        const product = firstValue(row.product, row.Product);
        const plant = firstValue(row.plant, row.Plant);
        const storageLocation = firstValue(
          row.storageLocation,
          row.StorageLocation,
        );
        const key = `${product}-${plant}-${storageLocation}`;
        const node = addNode(
          nodes,
          nodeIndex,
          "product_storage_location",
          key,
          row,
          key,
        );
        if (node) {
          productStorageLocationNodeByKey.set(key, node);
          addEdge(
            edges,
            edgeIndex,
            "product_has_storage_location",
            nodeId("product", product),
            node.id,
          );
          addEdge(
            edges,
            edgeIndex,
            "product_storage_location_to_plant",
            node.id,
            nodeId("plant", plant),
          );
        }
      });

      plants.forEach((row) => {
        const plant = firstValue(row.plant, row.Plant);
        const node = addNode(
          nodes,
          nodeIndex,
          "plant",
          plant,
          row,
          firstValue(row.plantName, plant),
        );
        if (node) {
          plantNodeById.set(plant, node);
        }
      });

      salesOrders.forEach((row) => {
        const orderId = firstValue(row.salesOrder, row.SalesOrder);
        const customerId = firstValue(row.soldToParty, row.SoldToParty);
        addEdge(
          edges,
          edgeIndex,
          "sales_order_customer",
          nodeId("sales_order", orderId),
          nodeId("business_partner", customerId),
        );
      });

      salesOrderItems.forEach((row) => {
        const orderId = firstValue(row.salesOrder, row.SalesOrder);
        const itemId = firstValue(row.salesOrderItem, row.SalesOrderItem);
        const product = firstValue(row.material, row.Material);
        const plant = firstValue(row.productionPlant, row.ProductionPlant);
        const storageLocation = firstValue(
          row.storageLocation,
          row.StorageLocation,
        );

        addEdge(
          edges,
          edgeIndex,
          "order_item_to_product",
          nodeId("sales_order_item", `${orderId}-${itemId}`),
          nodeId("product", product),
        );

        if (plant) {
          addEdge(
            edges,
            edgeIndex,
            "order_item_to_product_plant",
            nodeId("sales_order_item", `${orderId}-${itemId}`),
            nodeId("plant", plant),
          );
          addEdge(
            edges,
            edgeIndex,
            "order_item_to_plant_assignment",
            nodeId("sales_order_item", `${orderId}-${itemId}`),
            nodeId("product_plant", `${product}-${plant}`),
          );
        }

        if (storageLocation) {
          addEdge(
            edges,
            edgeIndex,
            "order_item_to_storage_location",
            nodeId("sales_order_item", `${orderId}-${itemId}`),
            nodeId(
              "product_storage_location",
              `${product}-${plant}-${storageLocation}`,
            ),
          );
        }
      });

      return { nodes, edges };
    })();
  }

  return cachedGraphPromise;
}

module.exports = buildGraph;
