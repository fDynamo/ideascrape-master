import axios from "axios";
import { SearchResultObj } from "./customTypes";

type APISearchReturn =
  | {
      isError: false;
      data: {
        results: SearchResultObj[];
        extra?: { generated_query_vector: string };
      };
    }
  | { isError: true; error: any };
export async function sendAPISearchRequest(
  args: any
): Promise<APISearchReturn> {
  const endpoint = process.env.NEXT_PUBLIC_FUNCTIONS_URL + "search_api";

  try {
    const res = await axios.post(endpoint, args, {
      headers: {
        "User-Agent": "hackersear.ch",
        Authorization: "Bearer " + process.env.NEXT_PUBLIC_FUNCTIONS_ANON_KEY,
      },
    });

    return { isError: false, data: res.data };
  } catch (error) {
    return { isError: true, error };
  }
}
