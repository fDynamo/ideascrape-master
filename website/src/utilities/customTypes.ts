export type SearchResultObj = {
  product_url: string;
  product_name: string;
  product_description: string;
  product_image_file_name: string;
  ph_source_url: string;
  ph_listed_at: string;
  ph_updated_at: string;
  sm_email: string | null;
  sm_instagram: string | null;
  sm_facebook: string | null;
  sm_twitter: string | null;
  sm_discord: string | null;
  sm_linkedin: string | null;
  sm_youtube: string | null;
  sm_tiktok: string | null;
  sm_reddit: string | null;
  popularity: string;
};

export type SearchResultExportObj = {
  product_url: string;
  product_name: string;
  product_description: string;
  listed_at: string;
  sm_email: string | null;
  sm_instagram: string | null;
  sm_facebook: string | null;
  sm_twitter: string | null;
  sm_discord: string | null;
  sm_linkedin: string | null;
  sm_youtube: string | null;
  sm_tiktok: string | null;
  sm_reddit: string | null;
  popularity: string;
};
