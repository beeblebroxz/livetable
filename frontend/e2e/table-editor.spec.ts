import { expect, test, type Page } from '@playwright/test';

async function openEditor(page: Page) {
  await page.goto('/#editor');
  await expect(page.getByText('Live connection', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Add row', exact: true })).toBeEnabled();
}

test('editor shares confirmed edits, supports keyboard navigation, and keeps sorting local', async ({ page, context }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await openEditor(page);
  const peer = await context.newPage();
  await openEditor(peer);
  const widgetRow = page.locator('tbody tr').filter({ has: page.locator('input[value="Widget"]') });
  const amount = widgetRow.getByRole('textbox', { name: /^Edit amount/ });
  const label = await amount.getAttribute('aria-label');
  const original = await amount.inputValue();
  await amount.fill('321.5');
  await amount.press('Escape');
  await expect(amount).toHaveValue(original);
  await expect(peer.getByLabel(label!)).toHaveValue(original);
  await amount.fill('321.5');
  await amount.press('Enter');
  await expect(page.getByRole('status')).toContainText('Change confirmed');
  await expect(peer.getByLabel(label!)).toHaveValue('321.5');
  await page.getByRole('button', { name: 'Sort by amount' }).click();
  await expect(page.locator('th[aria-sort="descending"]')).toContainText('amount');
  await page.getByLabel('Search table').fill('Widget');
  await expect(page.locator('tbody tr')).toHaveCount(1);
  await expect(peer.locator('tbody tr')).toHaveCount(2);
  await page.getByLabel(label!).fill(original);
  await page.getByLabel(label!).press('Enter');
  await expect(peer.getByLabel(label!)).toHaveValue(original);
  await page.getByLabel('Search table').fill('');

  // A rejected non-nullable value must remain confirmed and editable.
  await page.getByLabel(label!).fill('');
  await page.getByLabel(label!).press('Enter');
  await expect(page.getByRole('alert')).toBeVisible();
  await expect(page.getByLabel(label!)).toHaveValue(original);
  await page.getByRole('button', { name: 'Dismiss' }).click();
  await page.screenshot({ path: 'test-results/table-editor-desktop.png', fullPage: true });
  expect(errors).toEqual([]);
  await peer.close();
});

test('editor creates and removes shared rows with deliberate confirmation', async ({ page, context }) => {
  await openEditor(page);
  const peer = await context.newPage();
  await openEditor(peer);
  await page.getByRole('button', { name: 'Add row', exact: true }).click();
  await page.getByLabel('New region').fill('North');
  await page.getByLabel('New product').fill('Editor test record');
  await page.getByLabel('New amount').fill('750.25');
  await expect(peer.locator('tbody tr')).toHaveCount(2);
  await page.getByRole('button', { name: 'Create row' }).click();
  await expect(page.getByRole('status')).toContainText('New row confirmed');
  await expect(peer.locator('input[value="Editor test record"]')).toBeVisible();
  await page.getByRole('button', { name: 'Delete selected row' }).click();
  await expect(page.getByRole('alertdialog')).toContainText('There is no undo');
  await page.getByRole('button', { name: 'Keep row' }).click();
  await expect(peer.locator('input[value="Editor test record"]')).toBeVisible();
  await page.getByRole('button', { name: 'Delete selected row' }).click();
  await page.getByRole('button', { name: 'Delete permanently' }).click();
  await expect(page.getByRole('status')).toContainText('Row removed');
  await expect(peer.locator('input[value="Editor test record"]')).toHaveCount(0);
  await expect(peer.locator('tbody tr')).toHaveCount(2);
  await peer.close();
});

test('editor fits mobile and opens its new-row form with keyboard focus', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openEditor(page);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole('button', { name: 'Add row', exact: true }).click();
  await expect(page.getByLabel('New region')).toBeFocused();
  await page.getByRole('button', { name: 'Cancel new row' }).click();
  await page.getByLabel('Search table').fill('no-such-product');
  await expect(page.getByRole('heading', { name: 'No matching rows.' })).toBeVisible();
  await page.getByRole('button', { name: 'Clear search' }).click();
  await expect(page.locator('tbody tr')).toHaveCount(2);
  await page.screenshot({ path: 'test-results/table-editor-mobile.png', fullPage: true });
});
