import smtplib
import requests
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

class AlertSystem:
    def __init__(self):
        self.logger = self._setup_logger()
        
        # Configuración de alertas (en producción usar variables de entorno)
        self.email_config = {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "sender_email": "alerts@yourcompany.com",
            "sender_password": "your_app_password",
            "recipients": ["data-team@yourcompany.com"]
        }
        
        self.slack_webhook = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        handler = logging.FileHandler("/opt/airflow/data/logs/alerts.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def send_email_alert(self, subject, message, alert_level="INFO"):
        """Envía alerta por email"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config["sender_email"]
            msg['To'] = ", ".join(self.email_config["recipients"])
            msg['Subject'] = f"[{alert_level}] Data Pipeline Alert: {subject}"
            
            body = f"""
            Data Pipeline Alert
            
            Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            Alert Level: {alert_level}
            Subject: {subject}
            
            Details:
            {message}
            
            ---
            Automated alert from Data Pipeline Monitoring System
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # En desarrollo, solo registrar en logs
            self.logger.info(f"EMAIL ALERT would be sent: {subject}")
            self.logger.info(f"Message: {message}")
            
            # Código para envío real (descomentado en producción)
            # server = smtplib.SMTP(self.email_config["smtp_server"], self.email_config["smtp_port"])
            # server.starttls()
            # server.login(self.email_config["sender_email"], self.email_config["sender_password"])
            # server.sendmail(self.email_config["sender_email"], self.email_config["recipients"], msg.as_string())
            # server.quit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending email alert: {str(e)}")
            return False

    def send_slack_alert(self, message, alert_level="INFO"):
        """Envía alerta a Slack"""
        try:
            color_map = {
                "INFO": "#36a64f",      # Verde
                "WARNING": "#ff9500",   # Naranja  
                "ERROR": "#ff0000",     # Rojo
                "CRITICAL": "#8B0000"   # Rojo oscuro
            }
            
            slack_message = {
                "attachments": [
                    {
                        "color": color_map.get(alert_level, "#36a64f"),
                        "fields": [
                            {
                                "title": f"Data Pipeline Alert - {alert_level}",
                                "value": message,
                                "short": False
                            },
                            {
                                "title": "Timestamp",
                                "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                "short": True
                            }
                        ]
                    }
                ]
            }
            
            # En desarrollo, solo registrar en logs
            self.logger.info(f"SLACK ALERT would be sent: {message}")
            
            # Código para envío real (descomentado en producción)
            # response = requests.post(self.slack_webhook, json=slack_message)
            # response.raise_for_status()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending Slack alert: {str(e)}")
            return False

    def send_pipeline_failure_alert(self, task_name, error_message):
        """Envía alerta específica para fallos del pipeline"""
        subject = f"Pipeline Task Failed: {task_name}"
        message = f"""
        The data pipeline task '{task_name}' has failed with the following error:
        
        Error: {error_message}
        
        Please check the Airflow logs for more details and take corrective action.
        """
        
        self.send_email_alert(subject, message, "ERROR")
        self.send_slack_alert(f"Pipeline task '{task_name}' failed: {error_message}", "ERROR")

    def send_data_quality_alert(self, quality_results):
        """Envía alerta para problemas de calidad de datos"""
        if quality_results.get("critical_failures", 0) > 0:
            subject = "Data Quality Critical Failures Detected"
            message = f"""
            Critical data quality issues have been detected:
            
            - Total Checks: {quality_results.get('total_checks', 0)}
            - Failed Checks: {quality_results.get('failed_checks', 0)}
            - Critical Failures: {quality_results.get('critical_failures', 0)}
            
            Please investigate and resolve these issues immediately.
            """
            
            self.send_email_alert(subject, message, "CRITICAL")
            self.send_slack_alert(message, "CRITICAL")
        
        elif quality_results.get("warning_checks", 0) > 0:
            subject = "Data Quality Warnings"
            message = f"""
            Data quality warnings detected:
            
            - Warning Checks: {quality_results.get('warning_checks', 0)}
            
            These should be reviewed but are not critical.
            """
            
            self.send_slack_alert(message, "WARNING")

    def send_success_notification(self):
        """Envía notificación de éxito del pipeline"""
        message = f"""
        ✅ Data pipeline completed successfully!
        
        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        All tasks executed without errors and data quality checks passed.
        """
        
        self.logger.info("Pipeline completed successfully")
        self.send_slack_alert(message, "INFO")