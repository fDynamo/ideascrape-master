const axios = require("axios");

const OPERATION_NAME = "HomePage";
const MAIN_QUERY = `
query HomePage($cursor: String, $kind: HomefeedKindEnum!) {
  homefeed(after: $cursor, kind: $kind) {
    kind
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        date
        items {
          ... on Post {
            thumbnailImageUuid
            product {
              websiteUrl
              structuredData
              followersCount
              url
              tagline
              postsCount
              reviewsCount
              reviewersCount
              reviewsRating
              description
            }
          }
        }
      }
    }
  }
}
`;

/**
 * Cursor index 0 is today
 * 1 is yesterday
 * 2 is 2 days ago
 * and so on
 */
async function queryPH(cursor) {
  if (cursor === 0) {
    cursor = null;
  } else {
    cursor = cursor - 1;
  }
  const variables = {
    kind: "ALL",
    cursor: "" + cursor,
  };
  const res = await axios.post("https://www.producthunt.com/frontend/graphql", {
    operationName: OPERATION_NAME,
    variables,
    query: MAIN_QUERY,
  });

  return res.data;
}

module.exports = {
  queryPH,
};
