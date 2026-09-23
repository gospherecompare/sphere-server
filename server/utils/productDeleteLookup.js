async function resolveProductIdByEntityOrProductId(client, { tableName, targetId }) {
  if (!client || !tableName || !Number.isInteger(targetId) || targetId <= 0) {
    return null;
  }

  const firstQuery = await client.query(
    `SELECT product_id FROM ${tableName} WHERE product_id = $1 LIMIT 1`,
    [targetId],
  );
  if (firstQuery.rows.length) {
    return Number(firstQuery.rows[0].product_id);
  }

  const fallbackQuery = await client.query(
    `SELECT product_id FROM ${tableName} WHERE id = $1 LIMIT 1`,
    [targetId],
  );
  if (fallbackQuery.rows.length) {
    return Number(fallbackQuery.rows[0].product_id);
  }

  return null;
}

module.exports = {
  resolveProductIdByEntityOrProductId,
};
