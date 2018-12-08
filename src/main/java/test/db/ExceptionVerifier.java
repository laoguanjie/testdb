package test.db;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.code.regexp.Matcher;

public class ExceptionVerifier {

	private static Logger logger = LoggerFactory.getLogger(ExceptionVerifier.class);

	private final ConcurrentHashMap<ExceptionType, AtomicInteger> correctCounter = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<ExceptionType, AtomicInteger> wrongCounter = new ConcurrentHashMap<>();

	private final AtomicInteger dataExistCounter = new AtomicInteger(0);

	private final AtomicInteger dataNotExistCounter = new AtomicInteger(0);

	public void dataExist() {
		dataExistCounter.incrementAndGet();
	}

	public void dataNotExist() {
		dataNotExistCounter.incrementAndGet();
	}

	public void verify(Exception exception, Sql sql, Collection<Schema> latestSchemaList, SchemaLine schemaLine) {
		String errorMsg = exception.getMessage();
		for (ExceptionType t : ExceptionType.values()) {
			Matcher mat = t.getRule().matcher(errorMsg);
			if (!mat.find()) {
				continue;
			}

			Map<String, String> namedGroups = mat.namedGroups();

			boolean result = t.verify(sql, latestSchemaList, namedGroups, schemaLine);
			if (result == true) {
				if (!correctCounter.containsKey(t)) {
					correctCounter.putIfAbsent(t, new AtomicInteger(0));
				}
				correctCounter.get(t).incrementAndGet();
			} else {

				if (!wrongCounter.containsKey(t)) {
					wrongCounter.putIfAbsent(t, new AtomicInteger(0));
				}
				wrongCounter.get(t).incrementAndGet();
			}
			return;
		}

		logger.warn("unknown exception, errMsg={}, sql={}", exception.getMessage(), sql.getQuery());
	}

	public void printResult() {
		
		logger.info("========== 正确写入情况 ==========");
		logger.info("执行写入 {} 行中: {} 行验证写入成功，{} 行验证写入失败。", dataExistCounter.get() + dataNotExistCounter.get(),
				dataExistCounter.get(), dataNotExistCounter.get());
		
		logger.info("========== 正确写入异常情况 ==========");
		if (correctCounter.size() == 0) {
			logger.info("没正确异常发生。");
		} else {
			for (Map.Entry<ExceptionType, AtomicInteger> entry : correctCounter.entrySet()) {
				logger.info("{} 正确地发生 {} 次。", entry.getKey(), entry.getValue());
			}
		}

		logger.info("========== 错误写入异常情况 ==========");
		if (wrongCounter.size() == 0) {
			logger.warn("没错误异常发生。");
		} else {
			for (Map.Entry<ExceptionType, AtomicInteger> entry : wrongCounter.entrySet()) {
				logger.warn("{} 错误地发生 {} 次。", entry.getKey(), entry.getValue());
			}
		}

	}
}
