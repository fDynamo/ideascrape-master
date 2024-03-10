function getAllComments(node) {
  if (node.nodeType === Node.COMMENT_NODE) {
    return [node.nodeValue];
  }

  if (node.childNodes) {
    const toReturn = [];
    for (var i = 0; i < node.childNodes.length; i++) {
      const res = getAllComments(node.childNodes[i]);
      res.forEach((el) => {
        if (el) toReturn.push(el);
      });
    }
    return toReturn;
  } else return [];
}

module.exports = {
  getAllComments,
};
