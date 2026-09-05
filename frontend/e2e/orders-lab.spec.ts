import { expect, test, type Page } from '@playwright/test';

async function ready(page: Page) {
  await expect(page.getByText('Local server connected')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Run excluded edit' })).toBeEnabled();
}

async function reset(page: Page, size = '1000') {
  await page.getByLabel('Dataset size').selectOption(size);
  await page.getByRole('button', { name: 'Reset / load' }).click();
  await expect(page.getByRole('alertdialog')).toBeVisible();
  await page.getByRole('button', { name: 'Reset shared lab' }).click();
  await expect(page.locator('.lab-result')).toContainText('Dataset reset');
  await expect(page.locator('.pipeline-node').filter({ hasText: 'All orders' }).locator('.node-bottom b')).toHaveText(Number(size).toLocaleString('en-US'));
}

test('guided tour exercises actual deltas, sorted moves and final-message recovery', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('/');
  await ready(page);
  await reset(page);
  await page.getByRole('button', { name: 'Run excluded edit' }).click();
  await expect(page.locator('.lab-result')).toContainText('Change less. Send less.');
  await expect(page.locator('.pipeline-node').filter({ hasText: 'All orders' }).locator('.delivery-tag')).toHaveText('delta');
  await expect(page.locator('.pipeline-node').filter({ hasText: 'High-value orders' }).locator('.delivery-tag')).toHaveText('no delivery');
  await expect(page.locator('.pipeline-node').filter({ hasText: 'Ranked orders' }).locator('.delivery-tag')).toHaveText('no delivery');
  await page.getByRole('button', { name: '02 Incremental membership' }).click();
  await page.getByRole('button', { name: 'Promote an order' }).click();
  await expect(page.locator('.lab-result')).toContainText('Cross the threshold.');
  await expect(page.locator('.pipeline-node').filter({ hasText: 'High-value orders' }).locator('.delivery-tag')).toHaveText('delta');
  await page.getByRole('button', { name: '03 Incremental ordering' }).click();
  await page.getByRole('button', { name: 'Move an order to #1' }).click();
  await expect(page.locator('.lab-result')).toContainText('Make a move.');
  await page.locator('.trace-entry.delta').filter({ hasText: 'Ranked orders' }).first().click();
  await expect(page.locator('.trace-detail')).toContainText('delete');
  await expect(page.locator('.trace-detail')).toContainText('insert');
  await page.getByRole('button', { name: '05 Snapshot recovery' }).click();
  await page.getByRole('button', { name: 'Drop a delta & recover' }).click();
  await expect(page.locator('.lab-result')).toContainText('Miss a message. Catch up.');
  await expect(page.locator('.trace-entry.dropped')).toHaveCount(1);
  await expect(page.locator('.trace-entry.repair')).toHaveCount(1);
  await expect(page.locator('.trace-entry.recovered')).toHaveCount(1);
  await page.screenshot({ path: 'test-results/orders-lab-desktop.png', fullPage: true });
  expect(errors).toEqual([]);
});

test('independent clients share resets and mutations without sharing filters', async ({ page, context }) => {
  await page.goto('/');
  await ready(page);
  await reset(page);
  const peer = await context.newPage();
  await peer.goto('/?client=b');
  await ready(peer);
  await expect(peer.getByLabel('Client filter threshold')).toHaveValue('2500');
  await expect(page.getByLabel('Client filter threshold')).toHaveValue('1000');
  const highCount = (target: Page) => target.locator('.pipeline-node').filter({ hasText: 'High-value orders' }).locator('.node-bottom b');
  expect(await highCount(page).textContent()).not.toBe(await highCount(peer).textContent());
  await page.locator('.pipeline-node').filter({ hasText: 'All orders' }).click();
  // Use the visible node title, not a derived row identity, for source editing.
  await peer.locator('.pipeline-node').filter({ hasText: 'All orders' }).click();
  await page.getByRole('button', { name: 'ORD-10001', exact: true }).click();
  await page.getByLabel('Selected order value').fill('777');
  await page.getByRole('button', { name: 'Apply change' }).click();
  await expect(page.locator('.lab-result')).toContainText('Edited ORD-10001');
  await expect(peer.locator('tbody tr').filter({ hasText: 'ORD-10001' })).toContainText('$777');
  await reset(page, '10000');
  await expect(peer.locator('.pipeline-node').filter({ hasText: 'All orders' }).locator('.node-bottom b')).toHaveText('10,000');
  await expect(peer.locator('tbody tr').filter({ hasText: 'ORD-10001' })).toContainText('$240');
  await expect(peer.getByLabel('Client filter threshold')).toHaveValue('2500');
  await peer.close();
});

test('100k rows remain scrollable and mixed streaming is bounded and pausable', async ({ page }) => {
  await page.goto('/');
  await ready(page);
  await reset(page, '100000');
  await page.locator('.pipeline-node').filter({ hasText: 'All orders' }).click();
  expect(await page.locator('tbody tr').count()).toBeLessThan(26);
  await page.getByLabel('Scrollable results table').evaluate(element => { element.scrollTop = element.scrollHeight; });
  await expect(page.getByRole('button', { name: 'ORD-110000', exact: true })).toBeVisible();
  expect(await page.locator('tbody tr').count()).toBeLessThan(26);
  await page.getByRole('button', { name: 'Explore & stream' }).click();
  await page.getByRole('button', { name: 'Single step' }).click();
  await expect(page.locator('.lab-result')).toContainText('Single mixed batch');
  await page.getByRole('button', { name: 'Start stream' }).click();
  await expect(page.locator('.trace-entry.complete')).not.toHaveCount(1);
  await page.getByRole('button', { name: 'Pause stream' }).click();
  await expect(page.getByRole('button', { name: 'Start stream' })).toBeEnabled();
  await expect(page.locator('.pipeline-node').filter({ hasText: 'All orders' }).locator('.node-bottom b')).toHaveText('100,000');
  await page.screenshot({ path: 'test-results/orders-lab-100k.png', fullPage: true });
});

test('mobile layout fits, reset cancellation is non-mutating, and the editor still works', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await ready(page);
  await reset(page);
  await page.getByRole('button', { name: 'Reset / load' }).click();
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(page.getByRole('alertdialog')).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: 'test-results/orders-lab-mobile.png', fullPage: true });
  await page.getByRole('link', { name: 'Table editor' }).click();
  await expect(page.getByRole('heading', { name: 'LiveTable Editor' })).toBeVisible();
  await expect(page.locator('input[value="Widget"]')).toBeVisible();
  await page.getByRole('link', { name: 'Orders Lab' }).click();
  await ready(page);
});
