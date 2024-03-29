package io.javabrains.talesandtomesdataloader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.format.annotation.DateTimeFormat;

import connection.DataStaxAstraProperties;
import io.javabrains.talesandtomesdataloader.author.Author;
import io.javabrains.talesandtomesdataloader.author.AuthorRepository;
import io.javabrains.talesandtomesdataloader.book.Book;
import io.javabrains.talesandtomesdataloader.book.BookRepository;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class TalesAndTomesDataLoaderApplication {

	@Autowired AuthorRepository authorRepository;
	@Autowired BookRepository bookRepository;

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
		Path path = Paths.get(worksDumpLoaction);
		DateTimeFormatter dateFormat =  DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = java.nio.file.Files.lines(path)) {
			lines.forEach(line -> {
				//read and parse the line
				String jsonString = line.substring(line.indexOf("{"));	

				try{
					JSONObject jsonObject = new JSONObject(jsonString);
					// create the book object
					Book book = new Book();
					book.setName(jsonObject.optString("title"));

					book.setId(jsonObject.getString("key").replace("/works/", ""));
					JSONObject descriptionObj = jsonObject.optJSONObject("description");
					if (descriptionObj != null) {
						book.setDescription(descriptionObj.optString("value"));
					}
					JSONObject publisheObj = jsonObject.optJSONObject("created");
					if (publisheObj != null) {
						String dateStr = publisheObj.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}

					JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
					if (coversJSONArr != null) {
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < coversJSONArr.length(); i++) {
							coverIds.add(coversJSONArr.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
					if (authorsJSONArr != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorsJSONArr.length(); i++){
							String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author").getString("key")
								.replace("/authors/", "");
								authorIds.add(authorId);

						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
							.map(optionalAuthor -> {
								if (!optionalAuthor.isPresent()) return "Unknown Author";
								return optionalAuthor.get().getName();
							}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);	
					System.out.println("Saving book " + book.getName() + " . . .");
					bookRepository.save(book);

					}
					
					;



				} catch (JSONException e) {
					e.printStackTrace();
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}

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
