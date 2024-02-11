use gtk::{prelude::*, ApplicationWindow, Button, Label};
use gtk::{glib, Application};

mod database; 

const APP_ID : &str = "org.gtk_rs.Enkidu";

fn main() -> glib::ExitCode {
    let app = Application::builder().application_id(APP_ID).build();
    app.connect_activate(build_ui);
    app.run()
}

fn build_ui(app : &Application){
    let button = Button::builder().label("Press").margin_bottom(120).margin_start(120).build();

    let label = Label::builder().label("Hello ").margin_top(20).margin_bottom(20).margin_start(20).margin_end(20).build();

    button.connect_clicked(|btn| {
        btn.set_label("label")
    });

    let window = ApplicationWindow::builder().application(app).title("Enkidu").child(&button).child(&label).build();

    window.present();
}