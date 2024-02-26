const getPercentageString = (currIndex, startIndex, lastIndex) => {
  const normalizedIndex = currIndex - startIndex;
  const normalizedLastIndex = lastIndex - startIndex;
  const doneFraction = normalizedIndex / normalizedLastIndex;
  const donePercentage = doneFraction * 100;
  const donePercentageString = donePercentage.toFixed(2) + "%";
  return donePercentageString;
};

const cleanTextForCsv = (inText, options) => {
  if (!inText) return "";

  let newText = inText;
  newText = newText.replace(/","/g, " ");

  if (options) {
    if (options.removeNewLine) {
      newText = newText.replace(/\n/g, " ");
    }
  }

  newText = newText.trim();
  newText = newText.replace(/ +/g, " ");

  return newText;
};

module.exports = { getPercentageString, cleanTextForCsv };
