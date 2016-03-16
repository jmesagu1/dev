package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	private final JdbcTemplate jdbcTemplate;


	@Override
	public void afterJob(JobExecution jobExecution) {
		//if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");

			List<Long> results = jdbcTemplate.query("SELECT count(*) FROM people", (rs, rowNum) -> rs.getLong(1));


			results.stream()
					.forEach(x -> log.info("Found <" + x + "> in the database."));


		//}
	}
}
