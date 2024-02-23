function filterNone() {
  return NodeFilter.FILTER_ACCEPT;
}

function getAllComments(rootElem) {
  var comments = [];
  // Fourth argument, which is actually obsolete according to the DOM4 standard, is required in IE 11
  var iterator = document.createNodeIterator(
    rootElem,
    NodeFilter.SHOW_COMMENT,
    filterNone,
    false
  );
  var curNode;
  while ((curNode = iterator.nextNode())) {
    comments.push(curNode.nodeValue);
  }
  return comments;
}

module.exports = {
  getAllComments,
};
