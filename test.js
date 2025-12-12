function getLastMonth15th() {
  const today = new Date();
  const lastMonth = new Date(today.getFullYear(), today.getMonth() - 1, 15);
  return lastMonth;
}
function getLastMonthLastDate() {
  const today = new Date();
  const lastMonth = new Date(today.getFullYear(), today.getMonth(), 0);
  return lastMonth;
}
function checkFirstCycle() {
  const today = new Date();
  const currentDate = today.getDate();

  if (currentDate < 15) {
    return true;
  } else {
    return false;
  }
}
function addToIST(date) {
  // IST timezone offset is UTC +5:30
  const ISTOffset = 5.5 * 60 * 60 * 1000; // Convert hours to milliseconds

  // Adjust the date based on the IST offset
  const adjustedDate = new Date(date.getTime() + ISTOffset);
  return adjustedDate;
}

function getMonthYear(date) {
  // Create a date object for the input date
  const istDate = addToIST(date);

  // Get month and year
  const monthYear =
    istDate.toLocaleString("default", {month: "long"}) +
    "_" +
    istDate.getFullYear();

  return monthYear;
}

const checkCohort = (date) => {
  const istDate = addToIST(date);
  const day = parseInt(("0" + istDate.getDate()).slice(-2));
  if (day <= 15) return 1;
  return 2;
};

function generateMonthlyRanges(endDateUtc) {
  const endDate = new Date(endDateUtc);

  const IST_OFFSET_MINUTES = 5.5 * 60; // IST = UTC+5:30
  const ranges = [];

  const year = 2024;
  const month = 1; // February (0-based)

  // Calculate how many months to iterate until endDate
  const totalMonths =
    (endDate.getUTCFullYear() - year) * 12 +
    (endDate.getUTCMonth() - month) +
    1;

  for (let i = 0; i < totalMonths; i++) {
    const currentYear = year + Math.floor((month + i) / 12);
    const currentMonth = (month + i) % 12;

    // Start of current month 00:00 IST -> UTC
    const startUtc = new Date(Date.UTC(currentYear, currentMonth, 1, 0, 0, 0, 0));
    startUtc.setUTCMinutes(startUtc.getUTCMinutes() - IST_OFFSET_MINUTES);

    // Start of next month 00:00 IST -> UTC
    const nextMonthUtc = new Date(Date.UTC(currentYear, currentMonth + 1, 1, 0, 0, 0, 0));
    nextMonthUtc.setUTCMinutes(nextMonthUtc.getUTCMinutes() - IST_OFFSET_MINUTES);

    // End of current month 23:59:59.999 IST -> UTC
    const monthEndUtc = new Date(nextMonthUtc.getTime() - 1);

    // Final month handling
    const isFinalMonth =
      currentYear === endDate.getUTCFullYear() &&
      currentMonth === endDate.getUTCMonth();

    const rangeEnd = isFinalMonth ?
      endDate :
      monthEndUtc > endDate ?
      endDate :
      monthEndUtc;

    ranges.push({
      startTime: startUtc,
      endTime: rangeEnd,
    });

    if (isFinalMonth) break;
  }

  return ranges;
}

const isFirstCycle = checkFirstCycle();
const endDateInUtc = false ? getLastMonth15th() : getLastMonthLastDate();
endDateInUtc.setDate(endDateInUtc.getDate() + 1);

// Generate month ranges from Feb 2024 to endDateIST month
const monthRanges = generateMonthlyRanges(endDateInUtc);
console.log(monthRanges);