package io.javabrains.talesandtomesdataloader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.datastax.oss.driver.shaded.guava.common.io.Files;
import com.datastax.oss.protocol.internal.response.AuthChallenge;

import connection.DataStaxAstraProperties;
import io.javabrains.talesandtomesdataloader.author.Author;
import io.javabrains.talesandtomesdataloader.author.AuthorRepository;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class TalesAndTomesDataLoaderApplication {

	@Autowired AuthorRepository authorRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLoaction;

	@Value("${datadump.location.works}")
	private String worksDumpLoaction;

	public static void main(String[] args) {
		SpringApplication.run(TalesAndTomesDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		Path path = Paths.get(authorDumpLoaction);
		try (Stream<String> lines = java.nio.file.Files.lines(path)){
			lines.forEach(line-> {
				//read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject;
				try {
					jsonObject = new JSONObject(jsonString);
					//construct author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					//persist data
					System.out.println("Saving author " + author.getName() + ". . .");
					authorRepository.save(author);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

		} catch (IOException e){
			e.printStackTrace();
		}

	}

	private void initWorks(){

	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}

	// giving spring data cass the ability to connect to instance

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
