plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.maven.publish) apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "com.vanniktech.maven.publish")

    group   = "io.github.agent0876.raknetty"
    version = "1.0.2"

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

    extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral()
        signAllPublications()

        coordinates(
            groupId    = "io.github.agent0876.raknetty",
            artifactId = project.name,
            version    = "1.0.2"
        )

        pom {
            name.set(project.name)
            description.set("Netty-based RakNet protocol implementation in Kotlin")
            url.set("https://github.com/Agent0876/RakNetty")

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
                connection.set("scm:git:git://github.com/Agent0876/RakNetty.git")
                developerConnection.set("scm:git:ssh://github.com/Agent0876/RakNetty.git")
                url.set("https://github.com/Agent0876/RakNetty")
            }
        }
    }
}