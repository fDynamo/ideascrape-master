const { createClient } = require("@supabase/supabase-js");
const dotenv = require("dotenv");

function createSupabaseClient(isProd) {
  dotenv.config();
  if (isProd) {
    return createClient(
      process.env.SUPABASE_PROD_API_URL ?? "",
      process.env.SUPABASE_PROD_SUPER_KEY ?? ""
    );
  } else {
    return createClient(
      process.env.SUPABASE_API_URL ?? "",
      process.env.SUPABASE_SUPER_KEY ?? ""
    );
  }
}

module.exports = createSupabaseClient;
