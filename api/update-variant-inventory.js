export default async function handler(req, res) {
  const STORE = process.env.SHOPIFY_STORE_DOMAIN;
  const ADMIN_API_KEY = process.env.SHOPIFY_ADMIN_API_KEY;
  const API_VERSION = '2025-07';
  const DRY_RUN = req.query.dryRun === 'true';
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

  while (hasNextPage) {
    const query = `
      query GetVariants($cursor: String) {
        productVariants(first: 250, after: $cursor) {
          edges {
            cursor
            node {
              id
              product {
                id
              }
              inventoryItem {
                id
                inventoryLevels(first: 50) {
                  edges {
                    node {
                      available
                      location {
                        name
                      }
                    }
                  }
                }
              }
              metafield(namespace: "linedup", key: "inventory_json") {
                id
                value
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
      body: JSON.stringify({ query, variables: { cursor } }),
    });

    const json = await response.json();
    const variants = json.data.productVariants.edges;
    hasNextPage = json.data.productVariants.pageInfo.hasNextPage;
    cursor = variants[variants.length - 1]?.cursor;

    for (const edge of variants) {
      const variant = edge.node;

      const productId = variant.product?.id?.split('/').pop();
      if (FILTER_PRODUCT_IDS && !FILTER_PRODUCT_IDS.includes(productId)) continue;
      if (variant.metafield) continue;

      const levels = variant.inventoryItem?.inventoryLevels?.edges || [];
      const inventory = {};

      for (const level of levels) {
        const locName = level.node.location.name;
        inventory[locName] = level.node.available;
      }

      if (DRY_RUN) {
        console.log(`DRY RUN — Variant: ${variant.id}`, inventory);
        dryRunLog.push({ variantId: variant.id, inventory });
        continue;
      }

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

      const variables = {
        metafields: [
          {
            ownerId: variant.id,
            namespace: 'linedup',
            key: 'inventory_json',
            type: 'json',
            value: JSON.stringify(inventory),
          },
        ],
      };

      const writeRes = await fetch(endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify({ query: mutation, variables }),
      });

      const writeJson = await writeRes.json();
      if (writeJson.data?.metafieldsSet?.metafields?.length > 0) {
        console.log(`✅ Updated metafield for variant: ${variant.id}`);
        updatedCount++;
      } else {
        console.error(`❌ Failed to write metafield for variant: ${variant.id}`, writeJson);
      }
    }
  }

  if (DRY_RUN) {
    console.log(`DRY RUN complete. ${dryRunLog.length} variants would be updated.`);
    return res
      .status(200)
      .json({ message: `Dry run complete. ${dryRunLog.length} variants would be updated.`, variants: dryRunLog });
  }

  console.log(`🎉 Completed. Updated ${updatedCount} variants.`);
  return res.status(200).json({ message: `Updated ${updatedCount} variants.` });
}
