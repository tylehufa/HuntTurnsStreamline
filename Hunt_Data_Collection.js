const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const os = require('os');
console.log('Script starting...');
const folderName = process.argv[2];  // The folder name will be the third argument
console.log(`Working with folder: ${folderName}`);
const userProfilePath = os.homedir();
const AUTH_FILE = path.join(__dirname, 'auth.json');
const today = new Date();
if (process.pkg) {
  const browserPath = path.join(
    path.dirname(process.execPath),
    'playwright-browsers'
  );
  process.env.PLAYWRIGHT_BROWSERS_PATH = browserPath;
}

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



async function Login(context, page) {
    try {
        await page.goto('https://idp.federate.amazon.com/api/saml2/v1/idp-initiated?providerId=gm-janus-internal&target=https://quicksight.aws.amazon.com');
        await page.waitForLoadState('networkidle');
        await page.getByRole('button', { name: 'Show me more' }).click();
    await page.getByRole('button', { name: 'Next' }).click();
    await page.getByRole('button', { name: 'Next' }).click();
    await page.getByRole('button', { name: 'Next' }).dblclick()
    await page.getByRole('button', { name: 'Done' }).click()
    try {await page.getByRole('button', { name: 'Collapse' }).click();;}
    catch (error){console.log('Collapse button not found')}
    await page.getByRole('textbox', { name: 'Search' }).click();
        console.log('Waiting for Welcome')
        await page.waitForSelector('h1:has-text("Welcome")');
        console.log('Found welcome')
        // Check for welcome message
        const welcome = await page.getByRole('heading',{ name: 'Welcome'}).isVisible();
        if (welcome) {
            console.log('Welcome message is visible');
                    // Save the authentication state
        await context.storageState({ path: AUTH_FILE });
        console.log(`Authentication file saved to: ${AUTH_FILE}`);
        return true; // Return true instead of 'success'
        }
    } catch (error) {
        console.error('Login failed:', error);
        return false; // Return false on error
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
    let context = await browser.newContext();
    let page = await context.newPage();
    const filePath = path.join(userProfilePath,'Downloads',folderName,'data.json');
    const data = readJsonFileSync(filePath);
    console.log('json:', data);

// Extract to variables
const { MBL, Containers } = data;

// Or traditional way
const mbl = data.MBL;
const containers = data.Containers
    page.on('console', msg => console.log('Browser Log:', msg.text()));
    success = await Login(context,page)
    if (success) {
        console.log('Login successful');
            if (page) await page.close();
            if (context) await context.close();
    } else {
        console.log('Login failed');
        return;
    }
    context = await browser.newContext({ storageState: AUTH_FILE });
    page = await context.newPage()
    await page.goto("https://idp.federate.amazon.com/api/saml2/v1/idp-initiated?providerId=gm-janus-internal&target=https://quicksight.aws.amazon.com")
    await page.getByRole('textbox', { name: 'Search' }).click();
    await page.getByRole('textbox', { name: 'Search' }).fill('GM Control Tower_New Dashboard');
    await page.getByRole('textbox', { name: 'Search' }).press('Enter');
    await page.getByRole('row', { name: 'GM Control Tower_New Dashboard' }).getByRole('link').click();
    try{
    await page.getByRole('alert').getByRole('button', { name: 'close' }).click();}
    catch (error){console.log('Alert not found')}
    try {
    await page.getByRole('button', { name: 'close' }).click();}
    catch (error) {console.log('close not found')}
    await page.getByText('Shipment / EDI Search',{ exact:true }).click()
    await page.getByRole('textbox', { name: 'Container_id' }).click();
    await page.getByRole('textbox', { name: 'Container_id' }).clear()
    await page.getByRole('textbox', {name:'Container_id'}).fill(containers)
    await page.getByRole('textbox', { name: 'BOL' }).click();
    await page.getByRole('textbox', { name: 'BOL' }).clear();
    await page.getByRole('textbox', {name: 'BOL' }).fill(mbl)
    await page.getByRole('textbox', { name: 'Booking ID' }).click();
    await page.getByRole('textbox', { name: 'Booking ID' }).clear();
    await page.getByRole('textbox', { name: 'Booking ID' }).click();
    await page.getByRole('textbox', {name:'BOL'}).press('Enter')
    await page.getByRole('button', {name:'Table, Data'}).click()
    await page.getByRole('button', {name:'Menu options, Data, Table'}).click()
    await page.waitForLoadState('load', { timeout: 30000 })
    try {
    await page.getByRole("menuitem", { name: "Export to Excel" }).click();
            const exportAllFieldsButton = page.getByRole("menuitem", { name: "Export all fields to Excel" });
        if (await exportAllFieldsButton.isVisible()) {
            await exportAllFieldsButton.click();
        } else {
            console.log('Export all fields is not visible');
        }
    const downloadPromise2 = page.waitForEvent('download');
    const downloads = await downloadPromise2;
    await downloads.saveAs(path.join(userProfilePath,'Downloads', folderName,`Control_Tower_Data${formatDate(today)}.xlsx`))
    }
    catch (error){
        console.log(`No download started - likely no data${error}`)
    }
    let page2 = await context.newPage()
    await page2.goto("https://idp.federate.amazon.com/api/saml2/v1/idp-initiated?providerId=gm-janus-internal&target=https://quicksight.aws.amazon.com")
    await page2.getByRole('textbox', { name: 'Search' }).click();
    await page2.getByRole('textbox', { name: 'Search' }).fill('Global Mile NA Dray Carrier Performance');
    await page2.getByRole('textbox', { name: 'Search' }).press('Enter');
    await page2.getByRole('link', { name: 'Global Mile NA Dray Carrier Performance', exact: true }).click();
    try {
    await page2.getByRole('alert').getByRole('button', { name: 'close' }).click();}
    catch (error){console.log(`Alert not found ${error}`)}
    try {
        await page.waitForTimeout(1000)
    await page2.getByRole('button', { name: 'close' }).click();}
    catch (error) {console.log(`close not found ${error}`)}
    await page2.getByText('Dray Performance (Port)',{exact:true}).click()
    try{
        await page2.getByRole('button', {name:'Dray Carrier Options - Dray'}).first().click(timeout=5000)
    }
    catch (error) {
        console.error('An error occurred:', error)
    }
    // try{
    //     await page2.getByRole('button', { name:'Dray Carrier Options - Dray Carrier JB HUNT' }).click(timeout=5000)
    // }
    // catch(error){
    //     console.error('An error occurred:', error)
    // }
    if (await page2.getByRole('checkbox', {name:'Select all'}).isChecked()){
        await page2.getByRole('checkbox', {name:'Select all'}).uncheck()
        await page2.getByRole('listitem', {name:'JB HUNT'}).getByRole('checkbox').check()}

    if (await page2.getByRole('listitem', {name:'JB HUNT'}).getByRole('checkbox').isChecked())
    {await page2.locator("#menu- > div").first().click()
        console.log("Checked Proceeding to the next step")}

    else{
        await page2.getByRole('checkbox', {name:'Select all'}).check()
        await page2.getByRole('checkbox', {name:'Select all'}).uncheck()
        await page2.getByRole('listitem', {name:'JB HUNT'}).getByRole('checkbox').check()
    await page2.locator("#menu- > div").first().click()
    }

    await page2.getByRole('button', {name:'Pivot table, Containers'}).click()
    await page2.getByRole('button', {name:'Menu options, Containers'}).click()
    const downloadPromise = page2.waitForEvent('download');
    await page2.getByRole('menuitem', {name:'Export to Excel'}).click()
    const download2 = await downloadPromise
    await download2.saveAs(path.join(userProfilePath,'Downloads', folderName,`GM_NA_Carrier_Data ${formatDate(today)}.xlsx`))
    try {
        let page3= await context.newPage()
        await page3.goto("https://share.amazon.com/sites/gm-sustainability/_layouts/15/WopiFrame2.aspx?sourcedoc={01BF79EC-3F61-4083-AECB-79DA7CCA2F58}&file=2025_Project_Shazam_WBR.xlsx&action=default")
        await page3.locator('iframe[name="WebApplicationFrame"]').contentFrame().getByRole('tab', { name: 'Wk18' }).click();
        await page3.locator('iframe[name="WebApplicationFrame"]').contentFrame().getByRole('button', {name:'More'}).click(timeout=5000)
        const downloadPromise3 = page3.waitForEvent('download');
        await page3.locator('iframe[name="WebApplicationFrame"]').contentFrame().getByRole('menuitem', {name:'Download'}).click()
        const download4 = await downloadPromise3
        await download4.saveAs(path.join(userProfilePath,'Downloads', folderName, `2025_Project_Shazam_WBR ${formatDate(today)}.xlsx`))
    }
    catch (error) {
       console.error('An error occurred:', error)
    }

    finally {
    await browser.close();
    await context.close();
    await page.close();
    }
}


main().catch(console.error);