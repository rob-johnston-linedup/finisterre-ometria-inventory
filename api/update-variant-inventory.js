export default async function handler(req, res) {
  const STORE = process.env.SHOPIFY_STORE_DOMAIN;
  const ADMIN_API_KEY = process.env.SHOPIFY_ADMIN_API_KEY;
  const API_VERSION = '2025-07';
  const DRY_RUN = req.query.dryRun !== 'false'; // Dry run by default
  const FILTER_PRODUCT_IDS = req.query.productIds ? req.query.productIds.split(',').map((id) => id.trim()) : null;

  const endpoint = `https://${STORE}/admin/api/${API_VERSION}/graphql.json`;

  const headers = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': ADMIN_API_KEY,
  };

  let cursor = null;
  let hasNextPage = true;
  let updatedCount = 0;
  const dryRunLog = [];

  // Build search query for server-side filtering
  let searchQuery = 'NOT metafield_namespaces:linedup';
  if (FILTER_PRODUCT_IDS?.length) {
    const productQueries = FILTER_PRODUCT_IDS.map((id) => `product_id:${id}`).join(' OR ');
    searchQuery = `(${productQueries}) AND ${searchQuery}`;
  }

  // Single page fetch
  async function fetchPage(cursor) {
    const query = `
      query GetVariants($cursor: String, $search: String) {
        productVariants(first: 250, after: $cursor, query: $search) {
          edges {
            cursor
            node {
              id
              inventoryItem {
                inventoryLevels(first: 50) {
                  edges {
                    node {
                      available: quantities(names: "available") {
                        quantity
                      }
                      location {
                        name
                        address {
                          address1
                          zip
                          phone
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          pageInfo {
            hasNextPage
          }
        }
      }
    `;

    const response = await fetch(endpoint, {
      method: 'POST',
      headers,
      body: JSON.stringify({ query, variables: { cursor, search: searchQuery } }),
    });

    return response.json();
  }

  // Process and optionally write in batches
  async function processPage(variants) {
    const metafieldBatch = [];

    for (const edge of variants) {
      const variant = edge.node;
      const levels = variant.inventoryItem?.inventoryLevels?.edges || [];

      const inventory = levels.map((level) => ({
        store: {
          name: level.node.location.name,
          address1: level.node.location.address?.address1 || '',
          zip: level.node.location.address?.zip || '',
          phone: level.node.location.address?.phone || '',
        },
        available: level.node.available?.quantity ?? 0,
      }));

      if (DRY_RUN) {
        dryRunLog.push({ variantId: variant.id, inventory });
        continue;
      }

      metafieldBatch.push({
        ownerId: variant.id,
        namespace: 'linedup',
        key: 'inventory_json',
        type: 'json',
        value: JSON.stringify(inventory),
      });

      // If batch reaches 25, send immediately
      if (metafieldBatch.length === 25) {
        await writeBatch(metafieldBatch);
        metafieldBatch.length = 0;
      }
    }

    // Flush remaining in batch
    if (!DRY_RUN && metafieldBatch.length) {
      await writeBatch(metafieldBatch);
    }
  }

  // Batch writer
  async function writeBatch(batch) {
    const mutation = `
      mutation SetInventoryMetafield($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          metafields {
            id
            key
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const resBatch = await fetch(endpoint, {
      method: 'POST',
      headers,
      body: JSON.stringify({ query: mutation, variables: { metafields: batch } }),
    });

    const writeJson = await resBatch.json();
    const errors = writeJson.data?.metafieldsSet?.userErrors || [];
    if (errors.length) {
      console.error('Batch write errors:', errors);
    } else {
      updatedCount += batch.length;
      console.log(`✅ Updated ${batch.length} variants in one batch`);
    }
  }

  // Main loop
  while (hasNextPage) {
    const json = await fetchPage(cursor);
    const variants = json.data.productVariants.edges;
    hasNextPage = json.data.productVariants.pageInfo.hasNextPage;
    cursor = variants[variants.length - 1]?.cursor;

    await processPage(variants);
  }

  // Final response
  if (DRY_RUN) {
    console.log(`DRY RUN complete. ${dryRunLog.length} variants would be updated.`);
    return res.status(200).json({
      message: `Dry run complete. ${dryRunLog.length} variants would be updated.`,
      variants: dryRunLog,
    });
  } else {
    console.log(`🎉 Completed. Updated ${updatedCount} variants.`);
    return res.status(200).json({ message: `Updated ${updatedCount} variants.` });
  }
}
