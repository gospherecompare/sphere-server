"use strict";

const dateKey = (value = new Date()) =>
  new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value));

const reserveTvGenerationCall = async (
  db,
  { date = new Date(), maxGeminiCalls = 12 } = {},
) => {
  const result = await db.query(
    `INSERT INTO tv_generation_usage (usage_date, gemini_calls, successful_tvs)
     VALUES ($1::date, 1, 0)
     ON CONFLICT (usage_date) DO UPDATE
       SET gemini_calls = tv_generation_usage.gemini_calls + 1,
           updated_at = now()
       WHERE tv_generation_usage.gemini_calls < $2
     RETURNING usage_date, gemini_calls, successful_tvs`,
    [dateKey(date), maxGeminiCalls],
  );
  if (!result.rows.length) {
    const error = new Error("Daily TV Gemini call limit reached");
    error.code = "TV_GEMINI_QUOTA_EXCEEDED";
    throw error;
  }
  return result.rows[0];
};

const recordSuccessfulTv = async (
  db,
  { date = new Date(), maxSuccessfulTvs = 5 } = {},
) => {
  const result = await db.query(
    `UPDATE tv_generation_usage
     SET successful_tvs = successful_tvs + 1, updated_at = now()
     WHERE usage_date = $1::date AND successful_tvs < $2
     RETURNING usage_date, gemini_calls, successful_tvs`,
    [dateKey(date), maxSuccessfulTvs],
  );
  if (!result.rows.length) {
    const error = new Error("Daily successful TV limit reached");
    error.code = "TV_SUCCESS_QUOTA_EXCEEDED";
    throw error;
  }
  return result.rows[0];
};

module.exports = { dateKey, reserveTvGenerationCall, recordSuccessfulTv };
