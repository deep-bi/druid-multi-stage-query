package integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.ElementClickInterceptedException;
import org.openqa.selenium.JavascriptException;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

class DruidMsqResultsTest
{

  private static WebDriver driver;
  private static WebDriverWait wait;

  private static String baseUrl;
  private static String taskId;
  private static String expectedResultsFilePath;

  @BeforeAll
  static void setUp()
  {
    baseUrl = require("druidBaseUrl");
    taskId = require("taskId");
    expectedResultsFilePath = require("expectedResultsFilePath");
    boolean headless = Boolean.parseBoolean(propOr("headless", "true"));
    Duration timeout = Duration.ofSeconds(Long.parseLong(propOr("timeoutSec", "120")));

    if (!baseUrl.startsWith("http://") && !baseUrl.startsWith("https://")) {
      throw new IllegalArgumentException("druidBaseUrl must start with http:// or https://");
    }

    ChromeOptions options = new ChromeOptions();
    if (headless) {
      options.addArguments("--headless=new");
    }
    options.addArguments("--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080", "--disable-gpu");

    driver = new ChromeDriver(options);
    driver.manage().timeouts().implicitlyWait(Duration.ZERO);
    wait = new WebDriverWait(driver, timeout);
  }

  @AfterAll
  static void tearDown() {
    if (driver != null) {
      driver.quit();
    }
  }

  @Test
  void msqResultsAreVisibleInTasksView()
  {
    driver.navigate().to(trimSlash(baseUrl) + "/unified-console.html#tasks");
    waitForTasksView();

    filterByTaskId(taskId);

    assertTaskTypeNativeController(taskId);

    openTaskDetails(taskId);
    openResultsTab();

    int rows = waitForAnyResultRows();

    verifyResultsAgainstExpected(expectedResultsFilePath);

    Assertions.assertTrue(rows > 0, "No result rows for task " + taskId);
  }


  private static void waitForTasksView()
  {
    wait.until(d -> {
      if (d.getCurrentUrl().contains("#tasks")) {
        return true;
      }
      if (!d.findElements(By.cssSelector("a[href*='#tasks']")).isEmpty()) {
        return true;
      }
      if (!d.findElements(By.xpath("//a[normalize-space()='Tasks'] | //button[normalize-space()='Tasks']")).isEmpty()) {
        return true;
      }
      if (!d.findElements(By.cssSelector("div.rt-table")).isEmpty()) {
        return true;
      }
      return !d.findElements(By.cssSelector("table")).isEmpty();
    });
  }

  private static void filterByTaskId(String id)
  {
    WebElement container = wait.until(d -> {
      List<WebElement> rt = d.findElements(By.cssSelector("div.ReactTable"));
      if (!rt.isEmpty()) {
        return rt.get(0);
      }
      List<WebElement> tb = d.findElements(By.cssSelector("table"));
      return tb.isEmpty() ? null : tb.get(0);
    });

    if (container.getAttribute("class").contains("ReactTable")) {
      WebElement filtersRow = wait.until(d -> {
        List<WebElement> rows = d.findElements(By.cssSelector(".ReactTable .rt-thead.-filters .rt-tr"));
        return rows.isEmpty() ? null : rows.get(0);
      });

      WebElement input = null;
      try {
        input = filtersRow.findElement(By.cssSelector(".rt-th:nth-child(1) input"));
      }
      catch (NoSuchElementException ignore) {
        List<WebElement> inputs = filtersRow.findElements(By.cssSelector("input, .bp4-input-group input"));
        if (!inputs.isEmpty()) {
          input = inputs.get(0);
        }
      }
      if (input == null) {
        throw new AssertionError("Task ID filter input not found");
      }

      ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView({block:'center'});", input);
      input.click();
      input.sendKeys(id);
      input.sendKeys(Keys.ENTER);

      String lit = xlit(id);
      wait.until(d ->
                     !d.findElements(By.xpath("//div[contains(@class,'rt-tbody')]//a[contains(., " + lit + ")]"))
                       .isEmpty()
                     || !d.findElements(By.xpath(
                              "//div[contains(@class,'rt-tbody')]//*[self::div or self::span][contains(., " + lit + ")]"))
                          .isEmpty());
      return;
    }

    WebElement thead = container.findElement(By.tagName("thead"));
    WebElement filterRow = thead.findElement(By.xpath(".//tr[.//input]"));
    WebElement input = filterRow.findElement(By.xpath("(./th|./td)[1]//input"));
    ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView({block:'center'});", input);
    input.click();
    input.sendKeys(id);
    input.sendKeys(Keys.ENTER);

    String lit = xlit(id);
    wait.until(d ->
                   !d.findElements(By.xpath("//table//tbody//tr//a[contains(., " + lit + ")]")).isEmpty()
                   || !d.findElements(By.xpath("//table//tbody//tr//td[contains(., " + lit + ")]")).isEmpty());
  }

  private static void assertTaskTypeNativeController(String id) {
    WebElement row = findTasksRowByExactTaskId(id);
    String text = row.getText().toLowerCase(Locale.ROOT);
    Assertions.assertTrue(
        text.contains("native_query_controller"),
        "Expected native_query_controller, actual row: " + text);
  }

  private static WebElement findTasksRowByExactTaskId(String id) {
    String lit = xlit(id);
    return wait.until(d -> {
      List<WebElement> rt = d.findElements(By.xpath(
          "//div[contains(@class,'rt-tbody')]//div[contains(@class,'rt-tr-group')]"
          + "[.//div[contains(@class,'rt-td')][1]//a[normalize-space(.)=" + lit + "]"
          + " or .//div[contains(@class,'rt-td')][1][normalize-space(.)=" + lit + "]]"));
      if (!rt.isEmpty()) return rt.get(0);
      List<WebElement> tr = d.findElements(By.xpath(
          "//table//tbody//tr[./td[1]//a[normalize-space(.)=" + lit + "] or ./td[1][normalize-space(.)=" + lit + "]]"));
      return tr.isEmpty() ? null : tr.get(0);
    });
  }

  private static void openTaskDetails(String id)
  {
    String lit = xlit(id);

    Optional<WebElement> cell = firstPresent(
        By.xpath(
            "//div[contains(@class,'rt-tbody')]//div[contains(@class,'rt-tr-group')]//div[contains(@class,'rt-td')][1]//a[normalize-space(.)="
            + lit
            + "]"),
        By.xpath("//table//tbody//tr//td[1]//a[normalize-space(.)=" + lit + "]"),
        By.xpath(
            "//div[contains(@class,'rt-tbody')]//div[contains(@class,'rt-tr-group')]//div[contains(@class,'rt-td')][1][normalize-space(.)="
            + lit
            + "]"),
        By.xpath("//table//tbody//tr//td[1][normalize-space(.)=" + lit + "]")
    );

    WebElement target = cell.orElseThrow(() -> new AssertionError("Task ID cell not found"));
    scrollIntoView(target);
    safeClick(target);

    wait.until(d -> {
      List<WebElement> dlgs = d.findElements(By.xpath(
          "(//*[@role='dialog'][contains(@class,'bp5-dialog') or contains(@class,'bp4-dialog') or contains(@class,'execution-details-dialog')])[last()]"
      ));
      if (dlgs.isEmpty()) {
        return null;
      }

      WebElement dlg = dlgs.get(dlgs.size() - 1);
      if (!dlg.isDisplayed() || dlg.getRect().getHeight() < 200) {
        return null;
      }

      boolean contentReady =
          !dlg.findElements(By.cssSelector(".fancy-tab-pane.execution-details-pane")).isEmpty()
          && dlg.findElements(By.cssSelector(".bp5-skeleton, .bp5-spinner, .bp4-skeleton, .bp4-spinner")).isEmpty();
      if (!contentReady) {
        return null;
      }

      List<WebElement> res = dlg.findElements(By.xpath(
          ".//div[contains(@class,'side-bar')]//*[self::button or self::a]" +
          "[.//span[(contains(@class,'button-text') or contains(@class,'bp5-button-text') or contains(@class,'bp4-button-text'))" +
          " and normalize-space(.)='Results']]"
      ));
      if (!res.isEmpty() && res.get(0).isDisplayed() && res.get(0).isEnabled()) {
        return dlg;
      }

      return null;
    });
  }


  private static void openResultsTab()
  {
    WebElement tab = firstPresent(
        By.xpath("//button[normalize-space(.)='Results']"),
        By.xpath("//a[normalize-space(.)='Results']"),
        By.xpath("//*[contains(@class,'tab') and normalize-space(.)='Results']")
    ).orElseThrow(() -> new AssertionError("'Results' tab not found"));
    safeClick(tab);

    wait.until(d ->
                   !d.findElements(By.cssSelector("div.rt-tbody div.rt-tr-group")).isEmpty()
                   || !d.findElements(By.cssSelector("table tbody tr")).isEmpty()
                   || !d.findElements(By.xpath("//*[contains(.,'No results')]")).isEmpty());
  }

  private static int waitForAnyResultRows()
  {
    return wait.until(d -> {
      if (!d.findElements(By.xpath("//*[contains(.,'No results')]")).isEmpty()) {
        throw new AssertionError("Results tab shows 'No results'");
      }
      int a = d.findElements(By.cssSelector("div.rt-tbody div.rt-tr-group")).size();
      int b = d.findElements(By.cssSelector("table tbody tr")).size();
      int rows = Math.max(a, b);
      return rows > 0 ? rows : null;
    });
  }

  private static void verifyResultsAgainstExpected(String expectedFilePath) {
    WebElement dlg = findDetailsDialog();

    WebElement pane = dlg.findElements(By.cssSelector(".result-table-pane")).stream()
                         .findFirst().orElse(dlg);

    List<Map.Entry<String, Long>> actual = new ArrayList<>();
    List<WebElement> rows = pane.findElements(By.cssSelector("div.rt-tbody div.rt-tr-group"));
    if (!rows.isEmpty()) {
      for (WebElement g : rows) {
        List<WebElement> cells = g.findElements(By.cssSelector("div.rt-td"));

        if (cells.size() < 2) {
          continue;
        }

        String k = normalizeCountry(cells.get(0).getText());
        String vtxt = cells.get(1).getText().trim();

        // stop once blank rows start
        if ((k == null || k.isEmpty()) && vtxt.isEmpty()) {
          break;
        }

        long v = parseLong(vtxt);
        actual.add(new AbstractMap.SimpleEntry<>(k, v));
      }
    } else {
      for (WebElement tr : pane.findElements(By.cssSelector("table tbody tr"))) {
        List<WebElement> td = tr.findElements(By.cssSelector("td,th"));
        if (td.size() >= 2) {
          String k = normalizeCountry(td.get(0).getText());
          long v = parseLong(td.get(1).getText());
          actual.add(new AbstractMap.SimpleEntry<>(k, v));
        }
      }
    }

    if (expectedFilePath != null && !expectedFilePath.isBlank()) {
      List<Map.Entry<String, Long>> expected = parseExpectedPairs(Paths.get(expectedFilePath));
      Assertions.assertEquals(expected, actual, "Results differ from " + expectedFilePath);
    }
  }

  private static Optional<WebElement> firstPresent(By... locators)
  {
    for (By by : locators) {
      List<WebElement> els = driver.findElements(by);
      if (!els.isEmpty()) {
        return Optional.of(els.get(0));
      }
    }
    return Optional.empty();
  }

  private static void safeClick(WebElement el)
  {
    try {
      new WebDriverWait(driver, Duration.ofSeconds(10))
          .until(ExpectedConditions.elementToBeClickable(el));
      el.click();
    }
    catch (ElementClickInterceptedException e) {
      new Actions(driver).moveToElement(el).pause(Duration.ofMillis(100)).click().perform();
    }
  }

  private static void scrollIntoView(WebElement el)
  {
    try {
      ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView({block:'center'});", el);
    }
    catch (JavascriptException ignored) {
    }
  }

  private static String xlit(String s)
  {
    if (!s.contains("'")) {
      return "'" + s + "'";
    }
    if (!s.contains("\"")) {
      return "\"" + s + "\"";
    }
    String[] p = s.split("'");
    StringBuilder b = new StringBuilder("concat(");
    for (int i = 0; i < p.length; i++) {
      b.append("'").append(p[i]).append("'");
      if (i < p.length - 1) {
        b.append(",\"'\",");
      }
    }
    return b.append(")").toString();
  }

  private static String propOr(String k, String d)
  {
    String v = System.getProperty(k);
    return (v == null || v.isBlank()) ? d : v;
  }

  private static String require(String k)
  {
    String v = System.getProperty(k);
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException(k + " is required");
    }
    return v;
  }

  private static String trimSlash(String s)
  {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }


  private static WebElement findDetailsDialog() {
    return wait.until(d -> {
      List<WebElement> dl = d.findElements(By.xpath(
          "(//*[@role='dialog'][contains(@class,'bp5-dialog') or contains(@class,'bp4-dialog')])[last()]"));
      if (dl.isEmpty()) return null;
      WebElement el = dl.get(dl.size() - 1);
      return el.isDisplayed() && el.getRect().getHeight() > 200 ? el : null;
    });
  }
  private static String normalizeCountry(String s) {
    if (s == null) return null;
    String t = s.trim();
    return "null".equalsIgnoreCase(t) ? null : t;
  }
  private static long parseLong(String s) {
    String t = s.replace(",", "").trim();
    if (t.isEmpty()) return 0L;
    return Long.parseLong(t);
  }
  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
  private static List<Map.Entry<String, Long>> parseExpectedPairs(java.nio.file.Path p) {
    try {
      String content = Files.readString(p);
      Pattern pat = Pattern.compile(
          "\\{\\s*\"countryName\"\\s*:\\s*(null|\"(.*?)\")\\s*,\\s*\"rows\"\\s*:\\s*([0-9,]+)\\s*\\}");
      java.util.regex.Matcher m = pat.matcher(content);
      List<Map.Entry<String, Long>> out = new ArrayList<>();
      while (m.find()) {
        String name = (m.group(1).equals("null")) ? null : m.group(2);
        long cnt = parseLong(m.group(3));
        out.add(new AbstractMap.SimpleEntry<>(name, cnt));
      }
      return out;
    } catch (Exception e) {
      throw new AssertionError("Failed to read expected JSON: " + p + " -> " + e.getMessage(), e);
    }
  }
}
