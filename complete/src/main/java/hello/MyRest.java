package hello;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Created by jhon on 16/03/16.
 */

@Controller
public class MyRest {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobLauncher jobExecution;

     @Autowired
    public DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping("/dos")
    @ResponseBody
    String home2() {


        try(Stream<String> lines = Files.lines(Paths.get("src/main/resources/sample-data.csv"))){

            lines.parallel()
                    .forEach(
                            x ->
                            {
                                String [] dos = x.split(",");
                                jdbcTemplate.update("INSERT INTO people (first_name, last_name) VALUES ('" +  dos[0]  + "','" +
                                        dos[1] + "')");
                            }

                    );

        }
        catch (IOException e) {
            e.printStackTrace();
        }


        new JobCompletionNotificationListener(jdbcTemplate).afterJob(null);

        return "completed";
    }


    @RequestMapping("/")
    @ResponseBody
    String home() {

        JobExecution exc = null;

        Job job =  jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener( new JobCompletionNotificationListener(jdbcTemplate))
                .flow(step1())
                .end()
                .build();

        try {

            exc  =  jobExecution.run(job, new JobParameters());
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }

        return "Hello World!: " + exc.getStatus();
    }

    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(1000)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setDataSource(dataSource);
        writer.afterPropertiesSet();
        return writer;
    }

    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

}
