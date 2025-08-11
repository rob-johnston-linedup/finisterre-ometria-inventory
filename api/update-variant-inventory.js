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

  // Build search query to filter only variants without the metafield
  let searchQuery = 'NOT metafield_namespaces:linedup';
  if (FILTER_PRODUCT_IDS?.length) {
    const productQueries = FILTER_PRODUCT_IDS.map((id) => `product_id:${id}`).join(' OR ');
    searchQuery = `(${productQueries}) AND ${searchQuery}`;
  }

  console.log(`🔍 Starting inventory update...`);
  console.log(`   Dry run: ${DRY_RUN}`);
  console.log(`   Search query: ${searchQuery}`);

  // Fetch a single page of variants
  async function fetchPage(cursor) {
    console.log(`📡 Fetching page after cursor: ${cursor || '(start)'}`);
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
                      quantities(names: "available") {
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

    const json = await response.json();

    if (!json.data) {
      console.error('❌ GraphQL fetch error:', JSON.stringify(json, null, 2));
    }

    return json;
  }

  // Process variants and prepare metafield updates
  async function processPage(variants) {
    const metafieldBatch = [];

    for (const edge of variants) {
      const variant = edge.node;
      const levels = variant.inventoryItem?.inventoryLevels?.edges || [];

      console.log(`   Processing variant: ${variant.id}`);
      const inventory = levels.map((level) => {
        const availableQuantity = (level.node.quantities?.[0]?.quantity) || 0;

        console.log(`     Level JSON:`, level);
        console.log(`     Location: ${level.node.location.name} — Available: ${availableQuantity}`);

        return {
          store: {
            name: level.node.location.name,
            address1: level.node.location.address?.address1 || '',
            zip: level.node.location.address?.zip || '',
            phone: level.node.location.address?.phone || '',
          },
          available: availableQuantity,
        };
      });

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

      // Flush batch when full
      if (metafieldBatch.length === 25) {
        await writeBatch(metafieldBatch);
        metafieldBatch.length = 0;
      }
    }

    // Flush remaining metafields
    if (!DRY_RUN && metafieldBatch.length) {
      await writeBatch(metafieldBatch);
    }
  }

  // Send batch mutation to Shopify
  async function writeBatch(batch) {
    console.log(`📝 Writing batch of ${batch.length} metafields...`);
    const mutation = `
      mutation SetInventoryMetafield($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          metafields {
            id
            key
            value
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

    if (writeJson.data?.metafieldsSet?.userErrors?.length) {
      console.error('❌ Batch write userErrors:', JSON.stringify(writeJson.data.metafieldsSet.userErrors, null, 2));
    } else {
      updatedCount += batch.length;
      console.log(`✅ Updated ${batch.length} variants in this batch`);
    }
  }

  // Loop through pages of variants
  while (hasNextPage) {
    const json = await fetchPage(cursor);
    const variants = json.data?.productVariants?.edges || [];
    hasNextPage = json.data?.productVariants?.pageInfo?.hasNextPage;
    cursor = variants[variants.length - 1]?.cursor;

    await processPage(variants);
  }

  // Final output
  if (DRY_RUN) {
    console.log(`🏁 Dry run complete. ${dryRunLog.length} variants would be updated.`);
    return res.status(200).json({
      message: `Dry run complete. ${dryRunLog.length} variants would be updated.`,
      variants: dryRunLog,
    });
  } else {
    console.log(`🎉 Completed. Updated ${updatedCount} variants.`);
    return res.status(200).json({ message: `Updated ${updatedCount} variants.` });
  }
}
