plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    `maven-publish`
    signing
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    group   = "io.github.agent0876.raknetty"
    version = "0.1.0"

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

    // Sources & Javadoc JARs (Maven Central 필수 요건)
    val sourcesJar = tasks.register<Jar>("sourcesJar") {
        archiveClassifier.set("sources")
        from(project.the<SourceSetContainer>()["main"].allSource)
    }

    val javadocJar = tasks.register<Jar>("javadocJar") {
        archiveClassifier.set("javadoc")
        from(tasks.named("javadoc"))
    }

    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                artifact(sourcesJar)
                artifact(javadocJar)

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

        repositories {
            maven {
                name = "CentralPortal"
                // ⚠️ 주의: Central Portal 전용 주소로 Maven 배포를 시도할 때 404가 난다면 
                // 아래 S01 OSSRH 스테이징 주소를 사용해야 일반 maven-publish 플러그인이 정상 작동합니다.
                url = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = System.getenv("SONATYPE_USERNAME")
                    password = System.getenv("SONATYPE_PASSWORD")
                }
            }
        }
    }

    extensions.configure<SigningExtension> {
        // 1. 세 가지 변수를 모두 안전하게 환경 변수에서 가져옵니다
        val signingKeyId = System.getenv("SIGNING_KEY_ID")
        val signingKey = System.getenv("SIGNING_KEY")
        val signingPassword = System.getenv("SIGNING_PASSWORD")

        if (!signingKey.isNullOrBlank() && !signingPassword.isNullOrBlank()) {
            // 2. 인자 3개짜리 메서드를 사용하여 Key ID를 강제로 맵핑합니다.
            // 인자 순서: useInMemoryPgpKeys(keyId, secretKey, password)
            useInMemoryPgpKeys(signingKeyId, signingKey, signingPassword)
            sign(extensions.getByType<PublishingExtension>().publications["mavenJava"])
        } else {
            logger.warn("[$name] Signing credentials missing. Skipping signing task.")
        }
    }
}