const { chromium } = require ('playwright');
const path = require('path');
const fs = require('fs');
const  os = require('os'); 
const folderName = process.argv[2];  // The folder name will be the third argument
console.log(`Working with folder: ${folderName}`);
// --- Date Calculation ---
const filePath= path.join(os.homedir(),'Downloads',folderName,'data1.json'); 
const weeksToSubtract = 16; 
const today = new Date();
const targetDate = new Date(today);
targetDate.setDate(today.getDate() - (weeksToSubtract * 7));

const day = String(targetDate.getDate()).padStart(2, '0');
const month = String(targetDate.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
const year = targetDate.getFullYear();
const formattedTargetDate = `${month}/${day}/${year}`;


function readJsonFileSync(filePath) {
    try {
        const jsonString = fs.readFileSync(filePath, 'utf-8');
        const data = JSON.parse(jsonString);
        
        
        return data;
    } catch (error) {
        console.error('Error reading JSON file:', error);
        throw error;
    }
}

// As a function
function formatDate(date) {
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const day = date.getDate().toString().padStart(2, '0');
    const year = date.getFullYear().toString().slice(-2);
    return `${month}.${day}.${year}`;
}
 
async function main() { 
        const browser = await chromium.launch({
        headless: false,  // This makes the browser visible
        allowDownloads: true
    });
    const context = await browser.newContext();
    let page = await context.newPage();
   try{ 
      const data = readJsonFileSync(filePath);
    console.log('json:', data);

// Extract to variables
const { Containers } = data;
// Function to chunk array into groups of specified size
function chunkArray(array, chunkSize) {
    const chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
        chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
}

// Split Containers into chunks of 500
const containerChunks = chunkArray(Containers.split('\n'), 500);
  console.log(`Today's date: ${String(today.getMonth() + 1).padStart(2, '0')}/${String(today.getDate()).padStart(2, '0')}/${today.getFullYear()}`);
  console.log(`Target date (${weeksToSubtract} weeks ago): ${formattedTargetDate}`);
  await page.goto('https://reports.maerskwnd.com/Login.aspx?ReturnUrl=%2fdefault.aspx');
    // Create a countdown timer
  let timeLeft = 30;
  const timer = setInterval(() => {
      process.stdout.write(`\rTime remaining: ${timeLeft} seconds`);
      timeLeft--;
      if (timeLeft < 0) clearInterval(timer);
  }, 1000);
  await page.waitForTimeout(30000)
  await page.locator('#treeFrame').contentFrame().getByRole('link', { name: 'GM DCM Reports', exact: true }).click();
  await page.locator('#treeFrame').contentFrame().getByRole('link', { name: 'Inbound Container Milestone' }).click();
  await page.waitForTimeout(1000); // Wait for the page to load
  await page.locator('#rightFrame').contentFrame().locator('#ddlCustomer').selectOption('602^AMZ');
// Read from a text file containing your data
// Fill containers in chunks
await page.frameLocator('#rightFrame').locator('#txtContainers').fill('GVT Please Dont Delete Me')
await page.waitForTimeout(1000)
// Process chunks cumulatively
for (let i = 0; i < containerChunks.length; i++) {
    const chunk = containerChunks[i];
    console.log(`Adding chunk ${i + 1} of ${containerChunks.length}`);
    
    // Get existing value
    const existingValue = await page.frameLocator('#rightFrame')
        .locator('#txtContainers')
        .inputValue();
    
    // Combine existing value with new chunk
    const newValue = existingValue 
        ? `${existingValue}\n${chunk.join('\n')}`  // Add new chunk to existing
        : chunk.join('\n');  // First chunk
    
    // Fill with combined value
    await page.frameLocator('#rightFrame').locator('#txtContainers').click();
    await page.frameLocator('#rightFrame').locator('#txtContainers').fill(newValue);
    await page.frameLocator('#rightFrame').locator('#txtContainers').press('Enter');
    await page.waitForTimeout(1000);
}
await page.waitForTimeout(1000)
await page.locator('#rightFrame').contentFrame().locator('#tbStartDate').click()
  await page.locator('#rightFrame').contentFrame().locator('#tbStartDate').fill(formattedTargetDate);
  await page.waitForTimeout(1000)
  await page.locator('#rightFrame').contentFrame().getByRole('button', { name: 'Search' }).dblclick()
  await page.waitForTimeout(10000)
  await page.locator('#rightFrame').contentFrame().getByRole('button', { name: 'Export to Excel (w/o Format)' }).waitFor({ state: 'visible', timeout: 600000 });
  const download2Promise = page.waitForEvent('download');
  await page.locator('#rightFrame').contentFrame().getByRole('button', { name: 'Export to Excel (w/o Format)' }).click();
  const download3 = await download2Promise;
  const downloadsPath = path.join(require('os').homedir(),'Downloads', folderName);
  const gvtFilePath = path.join(downloadsPath, `GVT Search File ${formatDate(today)}.xlsx`);
  await download3.saveAs(gvtFilePath);
      
  console.log(`File saved as: ${gvtFilePath}`);
} catch (error) {
    console.error('An error occurred:', error);
} finally {
    await browser.close();
    await context.close();
    await page.close();
}}

// Run the program
main().catch(console.error);