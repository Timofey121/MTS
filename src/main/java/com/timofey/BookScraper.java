package com.timofey;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BookScraper {

    private static final String BASE_URL = "http://books.toscrape.com/";
    private static final String PAGE_URL = BASE_URL + "catalogue/page-%d.html";
    private static final int TOTAL_PAGES = 51;
    private static final int THREAD_COUNT = 10;
    private static final String OUTPUT_CSV = "books.csv";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        long startTime = System.currentTimeMillis();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_CSV))) {
            writer.write("title;price;availability;rating;bookUrl;description");
            writer.newLine();

            List<Future<Integer>> results = new ArrayList<>();

            System.out.println("Запуск парсинга с " + THREAD_COUNT + " потоками...");

            for (int i = 1; i <= TOTAL_PAGES; i++) {
                int pageNum = i;
                results.add(executor.submit(() -> parseCatalogPage(pageNum, writer)));
            }

            int totalBooks = 0;
            for (Future<Integer> result : results) {
                try {
                    totalBooks += result.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            long endTime = System.currentTimeMillis();
            long durationSec = (endTime - startTime) / 1000;

            System.out.println("Готово! Всего обработано книг: " + totalBooks);
            System.out.println("Время выполнения: " + durationSec + " секунд.");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static int parseCatalogPage(int pageNumber, BufferedWriter writer) {
        int count = 0;
        String catalogUrl = String.format(PAGE_URL, pageNumber);
        String threadName = Thread.currentThread().getName();

        try {
            Document doc = Jsoup.connect(catalogUrl).timeout(10_000).get();
            Elements bookElements = doc.select("article.product_pod");

            for (Element bookEl : bookElements) {
                String absLink = bookEl.selectFirst("h3 > a").absUrl("href");

                Map<String, String> bookData = parseBookDetail(absLink);
                if (bookData.isEmpty()) continue;

                synchronized (writer) {
                    writeBookToCsv(bookData, writer);
                }
                count++;
            }

            System.out.printf("[%s] Страница %d обработана, книг: %d%n", threadName, pageNumber, count);

        } catch (IOException e) {
            System.err.printf("[%s] Ошибка при парсинге страницы %d: %s%n", threadName, pageNumber, e.getMessage());
        }

        return count;
    }

    private static Map<String, String> parseBookDetail(String bookUrl) {
        Map<String, String> data = new HashMap<>();
        try {
            Document detailDoc = Jsoup.connect(bookUrl).timeout(10_000).get();

            String title = Optional.ofNullable(detailDoc.selectFirst("div.product_main > h1"))
                    .map(Element::text).orElse("");
            String price = Optional.ofNullable(detailDoc.selectFirst("div.product_main > p.price_color"))
                    .map(e -> e.text().trim()).orElse("");
            String availability =
                    Optional.ofNullable(detailDoc.selectFirst("div.product_main > p.instock.availability"))
                            .map(e -> e.text().trim()).orElse("");

            String ratingText = "";
            Element ratingEl = detailDoc.selectFirst("div.product_main > p.star-rating");
            if (ratingEl != null) {
                for (String cls : ratingEl.classNames()) {
                    if (List.of("One", "Two", "Three", "Four", "Five").contains(cls)) {
                        ratingText = cls;
                        break;
                    }
                }
            }
            int rating = ratingWordToNumber(ratingText);

            String description = "";
            Element descHeader = detailDoc.selectFirst("#product_description");
            if (descHeader != null) {
                Element descP = descHeader.nextElementSibling();
                if (descP != null && descP.tagName().equals("p")) {
                    description = descP.text().trim();
                }
            }

            data.put("title", title);
            data.put("price", price);
            data.put("availability", availability);
            data.put("rating", String.valueOf(rating));
            data.put("bookUrl", bookUrl);
            data.put("description", description);

        } catch (IOException e) {
            System.err.printf("Ошибка при парсинге detail-страницы: %s - %s%n", bookUrl, e.getMessage());
        }

        return data;
    }

    private static void writeBookToCsv(Map<String, String> bookData, BufferedWriter writer) throws IOException {
        String title = bookData.get("title").replace(";", ",");
        String price = bookData.get("price").replace(";", ",");
        String availability = bookData.get("availability").replace(";", ",");
        String rating = bookData.get("rating");
        String bookUrl = bookData.get("bookUrl").replace(";", ",");
        String description = bookData.get("description").replace(";", ",");

        String line = String.join(";", title, price, availability, rating, bookUrl, description);
        writer.write(line);
        writer.newLine();
    }

    private static int ratingWordToNumber(String word) {
        return switch (word.toLowerCase()) {
            case "one" -> 1;
            case "two" -> 2;
            case "three" -> 3;
            case "four" -> 4;
            case "five" -> 5;
            default -> 0;
        };
    }
}
