plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.maven.publish) apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    group   = "io.github.moltenmc.raknetty"
    version = "1.0.3"

    repositories {
        mavenCentral()
    }

    dependencies {
        "testImplementation"(kotlin("test"))
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(25)
    }

    if (project.name != "examples") {
        apply(plugin = "com.vanniktech.maven.publish")

        extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
            publishToMavenCentral()
            signAllPublications()

            coordinates(
                groupId    = "io.github.moltenmc.raknetty",
                artifactId = project.name,
                version    = "1.0.3"
            )

            pom {
                name.set(project.name)
                description.set("Netty-based RakNet protocol implementation in Kotlin")
                url.set("https://github.com/MoltenMC/RakNetty")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }

                developers {
                    developer {
                        id.set("agent0876")
                        name.set("Agent0876")
                        email.set("shinseungmin070920@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/MoltenMC/RakNetty.git")
                    developerConnection.set("scm:git:ssh://github.com/MoltenMC/RakNetty.git")
                    url.set("https://github.com/MoltenMC/RakNetty")
                }
            }
        }
    }
}