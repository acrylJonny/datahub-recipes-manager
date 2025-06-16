import logging
from django.db import connection


class DatabaseLogHandler(logging.Handler):
    """
    Custom log handler that stores logs in the database.
    """

    def emit(self, record):
        try:
            # Check if the log table exists before trying to use it
            if hasattr(connection, "introspection"):
                tables = connection.introspection.table_names()
                if "web_ui_logentry" not in tables:
                    return  # Skip logging to database if table doesn't exist yet

            # It's safe to import the model now
            from .models import LogEntry

            # Get the log message
            message = self.format(record)

            # Determine the source of the log
            if hasattr(record, "name"):
                source = record.name
            else:
                source = "application"

            # Get details if available (e.g., exception info)
            details = None
            if record.exc_info:
                details = self.formatter.formatException(record.exc_info)
            elif hasattr(record, "stack_info") and record.stack_info:
                details = self.formatter.formatStack(record.stack_info)

            # Create the log entry
            LogEntry.log(
                level=record.levelname, message=message, source=source, details=details
            )
        except Exception:
            # Never fail the application due to logging errors
            pass
