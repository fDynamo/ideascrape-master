const getPercentageString = (currIndex, startIndex, lastIndex) => {
  const normalizedIndex = currIndex - startIndex;
  const normalizedLastIndex = lastIndex - startIndex;
  const doneFraction = normalizedIndex / normalizedLastIndex;
  const donePercentage = doneFraction * 100;
  const donePercentageString = donePercentage.toFixed(2) + "%";
  return donePercentageString;
};
module.exports = { getPercentageString };
