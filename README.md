# ClickHouse & Flat File Data Ingestion Tool

A web-based application that facilitates bidirectional data ingestion between ClickHouse databases and flat files. The application supports JWT token-based authentication for ClickHouse, allows column selection, and provides data preview functionality.

## Features

- Bidirectional data transfer:
  - ClickHouse to Flat File
  - Flat File to ClickHouse
- JWT token-based authentication for ClickHouse
- Column selection for selective data ingestion
- Data preview functionality
- Support for table joins (bonus feature)
- Progress tracking
- Error handling with user-friendly messages

## Prerequisites

- Java 11 or higher
- Maven
- ClickHouse database (local or remote)
- Web browser with JavaScript enabled

## Setup

1. Clone the repository:

```bash
git clone <repository-url>
cd clickhouse-file-ingestion
```

2. Build the application:

```bash
mvn clean install
```

3. Run the application:

```bash
mvn spring-boot:run
```

The application will start on http://localhost:8080

## Usage

1. Select Data Source:

   - Choose between ClickHouse and Flat File as your data source

2. For ClickHouse as Source:

   - Enter connection details (Host, Port, Database, User, JWT Token)
   - Click "Connect" to fetch available tables
   - Select table(s) and desired columns
   - Optionally configure table joins
   - Preview data before ingestion
   - Choose target file location and format

3. For Flat File as Source:

   - Select the input file
   - Specify delimiter and header options
   - Configure ClickHouse connection details
   - Map columns and start ingestion

4. Monitor Progress:
   - Watch the progress bar during ingestion
   - View success/error messages
   - Check final record count

## Configuration

The application can be configured through `src/main/resources/application.properties`:

- `server.port`: Web server port (default: 8080)
- `spring.servlet.multipart.max-file-size`: Maximum file upload size
- `spring.servlet.multipart.max-request-size`: Maximum request size

## Security Considerations

- JWT tokens are transmitted securely
- File operations are performed in a temporary directory
- Input validation is implemented for all user inputs
- Error messages are sanitized

## Testing

Test the application using example datasets:

1. UK Price Paid dataset
2. Ontime dataset

Test cases included:

1. Single ClickHouse table to Flat File
2. Flat File to new ClickHouse table
3. Joined ClickHouse tables to Flat File
4. Connection/authentication failures
5. Data preview functionality

## Error Handling

The application provides detailed error messages for:

- Connection failures
- Authentication issues
- Invalid file formats
- Data type mismatches
- IO errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license]
